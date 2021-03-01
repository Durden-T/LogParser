package log_parser

import (
	"bytes"
	"context"
	"github.com/avast/retry-go"
	"github.com/fsnotify/fsnotify"
	jsoniter "github.com/json-iterator/go"
	"github.com/pkg/errors"
	"github.com/segmentio/kafka-go"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"io"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"test/log_parser/template_miner"
	"test/util"
	"time"
)

// const 默认持久化函数
var defaultPersistenceHandlerFun = template_miner.NewFilePersistenceHandler

type logTemplate struct {
	gorm.Model `json:"-"`

	ClusterId uint32 `gorm:"<-:create;not_null;uniqueIndex"`

	App    string `gorm:"<-:create;not_null"`
	Tokens string `gorm:"not_null"`
	Size   int64  `gorm:"not_null"` // 属于该模板的日志数量

	Level   string `gorm:"<-:create;not_null"` // 日志级别
	Content string `gorm:"not_null"`           // 样例内容
}

type logTemplateSlice []*logTemplate

type logParser struct {
	name string // 标记logParser

	consumer *kafka.Reader
	producer *kafka.Writer

	kafkaCfg *kafkaCfg

	templateMiner *template_miner.TemplateMiner

	resultBuffer logTemplateSlice        // 结果缓冲池
	bufferMap    map[uint32]*logTemplate // 判断是否存在
	bufferLock   sync.Mutex

	saveResultDuration time.Duration
	saveResultDone     chan<- struct{} // 用于关闭导出结果的ticker

	mysqlCfg *mysqlCfg
	db       *gorm.DB

	runnable sync.RWMutex // 互斥重载配置与处理日志操作
}

type kafkaCfg struct {
	Hosts             []string      `mapstructure:"hosts" yaml:"hosts"`
	InputTopic        string        `mapstructure:"input_topic" yaml:"input_topic"`
	OutPutTopic       string        `mapstructure:"output_topic" yaml:"output_topic"`
	ReadMinBytes      int           `mapstructure:"read_min_bytes" yaml:"read_min_bytes"`
	ReadMaxBytes      int           `mapstructure:"read_max_bytes" yaml:"read_max_bytes"`
	CommitInterval    time.Duration `mapstructure:"commit_interval" yaml:"commit_interval"`
	WriteBatchSize    int           `mapstructure:"write_batch_size" yaml:"write_batch_size"`
	WriteBatchBytes   int64         `mapstructure:"write_batch_bytes" yaml:"write_batch_bytes"`
	WriteBatchTimeout time.Duration `mapstructure:"write_batch_timeout" yaml:"write_batch_timeout"`
}

type mysqlCfg struct {
	Path      string `mapstructure:"path" json:"path" yaml:"path"`
	Config    string `mapstructure:"config" json:"config" yaml:"config"`
	Dbname    string `mapstructure:"db-name" json:"dbname" yaml:"db-name"`
	Username  string `mapstructure:"username" json:"username" yaml:"username"`
	Password  string `mapstructure:"password" json:"password" yaml:"password"`
	TableName string `mapstructure:"table-name" json:"table-name" yaml:"table-name"`
}

func New(configPath string) (*logParser, error) {
	logParser := &logParser{
		bufferMap: make(map[uint32]*logTemplate),
	}
	cfg, err := logParser.initConfig(configPath)
	if err != nil {
		log.Error(errors.WithMessage(err, "init config failed"))
		return nil, err
	}

	err = logParser.reloadFromConfig(cfg)
	if err != nil {
		log.Error(errors.WithMessage(err, "reload config failed"))
		return nil, err
	}

	// 尝试从持久化数据中恢复状态
	if err := logParser.templateMiner.LoadState(); err != nil {
		log.Error(errors.WithMessage(err, "load state failed"))
	}

	// 模拟析构函数
	runtime.SetFinalizer(logParser, logParserDestructor)
	return logParser, nil
}

// 从配置文件读取配置
func (l *logParser) initConfig(configPath string) (*viper.Viper, error) {
	log.Info("init log parser config")
	cfg := viper.New()
	cfg.SetConfigFile(configPath)
	err := cfg.ReadInConfig()
	if err != nil {
		return nil, err
	}

	// 配置文件变化时自动重载
	cfg.WatchConfig()
	cfg.OnConfigChange(func(e fsnotify.Event) {
		if err := l.reloadFromConfig(cfg); err != nil {
			log.Error(errors.WithMessage(err, "reload config failed"))
		}
	})

	return cfg, nil
}

// 根据配置文件重新初始化logParser
func (l *logParser) reloadFromConfig(cfg *viper.Viper) (err error) {
	log.Info("reload log parser")
	// 停止run
	l.runnable.Lock()
	defer l.runnable.Unlock()

	l.name = cfg.GetString("name")

	if l.templateMiner != nil {
		if err = l.templateMiner.ReloadFromConfig(cfg, defaultPersistenceHandlerFun); err != nil {
			err = errors.WithMessage(err, "reload template miner failed")
			log.Fatal(err)
		}
	} else if l.templateMiner, err = template_miner.New(cfg, defaultPersistenceHandlerFun); err != nil {
		err = errors.WithMessage(err, "init template miner failed")
		log.Fatal(err)
	}

	saveResultDuration := cfg.GetDuration("save_result_duration")
	if saveResultDuration != l.saveResultDuration {
		l.closeCron()
		// 定时保存结果
		l.saveResultDone = util.SetInterval(saveResultDuration, l.saveResult)
		l.saveResultDuration = saveResultDuration
	}

	// retry函数设置选项
	retryOpts := []retry.Option{
		retry.Delay(time.Second),
		retry.MaxJitter(time.Second),
		retry.LastErrorOnly(true),
		retry.OnRetry(func(n uint, err error) {
			log.Error(errors.WithMessagef(err, "retry for %d times", n))
		}),
	}

	var kafkaCfg kafkaCfg
	err = cfg.UnmarshalKey("kafka", &kafkaCfg)
	if err != nil {
		err = errors.WithMessage(err, "unmarshal kafka config failed")
		log.Fatal(err)
	}
	// 配置文件中kafka部分发生变化
	if !reflect.DeepEqual(l.kafkaCfg, kafkaCfg) {
		l.kafkaCfg = &kafkaCfg
		if err = retry.Do(l.initConsumer, retryOpts...); err != nil {
			err = errors.WithMessage(err, "init kafka consumer failed")
			log.Fatal(err)
		} else if err = retry.Do(l.initProducer, retryOpts...); err != nil {
			err = errors.WithMessage(err, "init kafka producer failed")
			log.Fatal(err)
		}
	}

	var mysqlCfg mysqlCfg
	err = cfg.UnmarshalKey("mysql", &mysqlCfg)
	if err != nil {
		err = errors.WithMessage(err, "unmarshal mysql config failed")
		log.Fatal(err)
		panic(err)
	}
	if !reflect.DeepEqual(l.mysqlCfg, mysqlCfg) {
		l.mysqlCfg = &mysqlCfg
		if err = retry.Do(l.initDB, retryOpts...); err != nil {
			err = errors.WithMessage(err, "init db failed")
			log.Fatal(err)
			panic(err)
		}
	}

	return
}

func (l *logParser) initConsumer() error {
	log.Info("init consumer")
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        l.kafkaCfg.Hosts,
		GroupID:        l.name,
		Topic:          l.kafkaCfg.InputTopic,
		MinBytes:       l.kafkaCfg.ReadMinBytes,
		MaxBytes:       l.kafkaCfg.ReadMaxBytes,
		CommitInterval: l.kafkaCfg.CommitInterval,
	})
	// 尝试fetch message, 不成功说明consumer初始化失败
	if _, err := r.FetchMessage(context.Background()); err != nil {
		return err
	}

	if l.consumer != nil {
		err := l.consumer.Close()
		if err != nil {
			return errors.WithMessage(err, "close old kafka consumer failed")
		}
	}

	l.consumer = r
	return nil
}

func (l *logParser) initProducer() error {
	log.Info("init producer")
	w := &kafka.Writer{
		Addr:         kafka.TCP(l.kafkaCfg.Hosts...),
		Topic:        l.kafkaCfg.OutPutTopic,
		RequiredAcks: kafka.RequireOne,
		Compression:  kafka.Lz4,
		Async:        true,
		BatchTimeout: l.kafkaCfg.WriteBatchTimeout,
		BatchSize:    l.kafkaCfg.WriteBatchSize,
		BatchBytes:   l.kafkaCfg.WriteBatchBytes,
	}

	l.producer = w
	return nil
}

func (l *logParser) initDB() error {
	log.Info("init db")
	mysqlCfg := l.mysqlCfg
	dsn := mysqlCfg.Username + ":" + mysqlCfg.Password + "@tcp(" + mysqlCfg.Path + ")/" + mysqlCfg.Dbname + "?" + mysqlCfg.Config
	mysqlConfig := mysql.Config{
		DSN:                       dsn,   // DSN data source name
		DisableDatetimePrecision:  true,  // 禁用 datetime 精度，MySQL 5.6 之前的数据库不支持
		DontSupportRenameIndex:    true,  // 重命名索引时采用删除并新建的方式，MySQL 5.7 之前的数据库和 MariaDB 不支持重命名索引
		DontSupportRenameColumn:   true,  // 用 `change` 重命名列，MySQL 8 之前的数据库和 MariaDB 不支持重命名列
		SkipInitializeWithVersion: false, // 根据版本自动配置
	}
	db, err := gorm.Open(mysql.New(mysqlConfig), nil)
	if err != nil {
		return errors.WithMessage(err, "open db failed")
	}
	// 指定表名 将数据结构迁移到数据库
	if err = db.Table(mysqlCfg.TableName).AutoMigrate(&logTemplate{}); err != nil {
		return errors.WithMessage(err, "migrate db table failed")
	}

	if l.db != nil {
		if mysqldb, err := l.db.DB(); err != nil {
			return errors.WithMessage(err, "get mysql db failed")
		} else if err = mysqldb.Close(); err != nil {
			return errors.WithMessage(err, "close mysql db failed")
		}
	}

	l.db = db
	return nil
}

// 模拟析构函数
func logParserDestructor(l *logParser) {
	if err := l.Close(); err != nil {
		log.Error(err)
	}
}

func (l *logParser) Close() error {
	// 关闭前保存剩余结果
	_ = l.saveResult()

	l.closeCron()
	if err := l.consumer.Close(); err != nil {
		return errors.WithMessage(err, "close consumer failed")
	}
	if err := l.producer.Close(); err != nil {
		return errors.WithMessage(err, "close producer failed")
	}

	if mysqldb, err := l.db.DB(); err != nil {
		return errors.WithMessage(err, "get mysql db failed")
	} else if err = mysqldb.Close(); err != nil {
		return errors.WithMessage(err, "close db failed")
	}
	return nil
}

// 关闭ticker 防止内存泄漏 见标准库中time.Ticker文档
func (l *logParser) closeCron() {
	if l.saveResultDone != nil {
		close(l.saveResultDone)
	}
}

func (l *logParser) Run() {
	log.Info("start running...")

	var wg sync.WaitGroup
	ctx := context.Background()
	// del
	start := time.Now()
	for {
		msg, err := l.consumer.ReadMessage(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Errorf("read message form kafka failed, err: %v", err)
			continue
		}

		// del
		if len(msg.Value) == 0 {
			break
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			l.ProcessLogMessage(msg.Value)
		}()
	}

	// del
	wg.Wait()
	_ = l.saveResult()
	log.Info(time.Since(start))
}

func (l *logParser) ProcessLogMessage(msg []byte) {
	logMessage := new(logTemplate)
	if err := jsoniter.Unmarshal(msg, logMessage); err != nil {
		log.Error("unmarshal message failed", err)
		return
	}

	logMessage.Size = 1

	// 防止此时重载配置
	l.runnable.RLock()
	defer l.runnable.RUnlock()

	cluster, level := l.templateMiner.ProcessLogMessage(logMessage.Content, logMessage.Level)
	logMessage.ClusterId = cluster.ID
	logMessage.Tokens = strings.Join(cluster.Tokens, " ")
	logMessage.Level = level // 归一化level

	l.saveToBuffer(logMessage)
	l.sendRealtimeResult(logMessage)
}

func (l *logParser) saveToBuffer(msg *logTemplate) {
	id := msg.ClusterId
	l.bufferLock.Lock()
	defer l.bufferLock.Unlock()

	if result, found := l.bufferMap[id]; found {
		result.Size++
		result.Tokens = msg.Tokens
		result.Content = msg.Content
		return
	}

	l.resultBuffer = append(l.resultBuffer, msg)
	l.bufferMap[id] = msg
}

// 将结果保存到数据库
func (l *logParser) saveResult() error {
	l.bufferLock.Lock()
	defer l.bufferLock.Unlock()

	if len(l.resultBuffer) == 0 {
		log.Info("no recent result")
		return nil
	}

	err := l.saveToDB()
	if err != nil {
		return errors.WithMessage(err, "save result to db failed")
	}

	// 清空buffer 但不重新分配内存
	l.resultBuffer = l.resultBuffer[:0]
	l.bufferMap = make(map[uint32]*logTemplate, len(l.bufferMap))
	return nil
}

func (l *logParser) sendRealtimeResult(template *logTemplate) {
	buf := bytes.NewBuffer([]byte{})
	// 防止转义字符
	enc := jsoniter.NewEncoder(buf)
	enc.SetEscapeHTML(false)

	if err := enc.Encode(template); err != nil {
		log.Error(err)
		return
	}
	msg := kafka.Message{
		Value: buf.Bytes(),
	}
	err := l.producer.WriteMessages(context.TODO(), msg)
	if err != nil {
		log.Error(err)
	}
}

func (l *logParser) saveToDB() (err error) {
	now := time.Now()
	defer func() {
		log.Infof("save result to db used %v", time.Since(now))
	}()
	clusterIds := make([]uint32, 0, len(l.resultBuffer))
	for key := range l.bufferMap {
		clusterIds = append(clusterIds, key)
	}

	var oldResults logTemplateSlice
	// 指定表名
	db := l.db.Table(l.mysqlCfg.TableName)
	// 显式使用事务
	tx := db.Begin()
	defer tx.Commit()
	// 获取数据库中的模板数据
	err = tx.Where("cluster_id in ?", clusterIds).Find(&oldResults).Error
	if err != nil {
		return
	}

	// 更新现有数据的Size
	for _, oldTemplate := range oldResults {
		if template, found := l.bufferMap[oldTemplate.ClusterId]; found {
			template.Size += oldTemplate.Size
		}
	}

	// Upsert 插入或更新，当cluster_id有冲突时更新指定列
	err = tx.Clauses(
		clause.OnConflict{
			Columns:   []clause.Column{{Name: "cluster_id"}},
			DoUpdates: clause.AssignmentColumns([]string{"tokens", "content", "size", "updated_at"}),
		},
	).Create(&l.resultBuffer).Error
	return
}
