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

// const
var defaultPersistenceHandlerFun = template_miner.NewFilePersistenceHandler

type logTemplate struct {
	gorm.Model `json:"-"`

	ClusterId uint32 `gorm:"<-:create;not_null;uniqueIndex"`

	App    string `gorm:"<-:create;not_null"`
	Tokens string `gorm:"not_null"`
	Size   int64  `gorm:"not_null"`

	Level   string `gorm:"<-:create;not_null"`
	Content string `gorm:"not_null"`
}

type logTemplateSlice []*logTemplate

type logParser struct {
	name string // 标记logParser

	consumer *kafka.Reader
	producer *kafka.Writer

	kafkaCfg *kafkaCfg

	templateMiner *template_miner.TemplateMiner

	resultBuffer logTemplateSlice // 结果缓冲池
	bufferMap    map[uint32]*logTemplate

	saveResultDuration time.Duration
	saveResultDone     chan<- struct{} // 用于关闭导出结果的ticker

	mysqlCfg *mysqlCfg
	db       *gorm.DB

	runnable sync.RWMutex // 互斥重载配置，导出结果与处理日志三种操作
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

	// 模拟析构函数 关闭ticker 防止内存泄漏
	runtime.SetFinalizer(logParser, logParserDestructor)
	return logParser, nil
}

func (l *logParser) initConfig(configPath string) (*viper.Viper, error) {
	log.Info("init log parser config")
	cfg := viper.New()
	cfg.SetConfigFile(configPath)
	err := cfg.ReadInConfig()
	if err != nil {
		return nil, err
	}

	cfg.WatchConfig()
	cfg.OnConfigChange(func(e fsnotify.Event) {
		if err := l.reloadFromConfig(cfg); err != nil {
			log.Error(errors.WithMessage(err, "reload config failed"))
		}
	})

	return cfg, nil
}

func (l *logParser) ProcessLogMessage(msg []byte) {
	logMessage := new(logTemplate)
	if err := jsoniter.Unmarshal(msg, logMessage); err != nil {
		log.Error("unmarshal message failed", err)
		return
	}

	logMessage.Size = 1

	l.runnable.RLock()
	defer l.runnable.RUnlock()

	cluster, level := l.templateMiner.ProcessLogMessage(logMessage.Content, logMessage.Level)
	logMessage.ClusterId = cluster.ID
	logMessage.Tokens = strings.Join(cluster.Tokens, " ")
	logMessage.Level = level

	l.saveToBuffer(logMessage)
	l.sendRealtimeResult(logMessage)
}

func (l *logParser) Close() error {
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

// 模拟析构函数 关闭ticker 防止内存泄漏
func logParserDestructor(l *logParser) {
	if err := l.Close(); err != nil {
		log.Error(err)
	}
}

func (l *logParser) closeCron() {
	if l.saveResultDone != nil {
		close(l.saveResultDone)
	}
}

func (l *logParser) reloadFromConfig(cfg *viper.Viper) (err error) {
	log.Info("reload log parser")

	l.runnable.Lock()
	defer l.runnable.Unlock()

	l.name = cfg.GetString("name")

	if l.templateMiner != nil {
		if err = l.templateMiner.ReloadFromConfig(cfg, defaultPersistenceHandlerFun); err != nil {
			err = errors.WithMessage(err, "reload template miner failed")
			log.Fatal(err)
			panic(err)
		}
	} else if l.templateMiner, err = template_miner.New(cfg, defaultPersistenceHandlerFun); err != nil {
		err = errors.WithMessage(err, "init template miner failed")
		log.Fatal(err)
		panic(err)
	}

	saveResultDuration := cfg.GetDuration("save_result_duration")
	if saveResultDuration != l.saveResultDuration {
		l.closeCron()
		l.saveResultDone = util.SetInterval(saveResultDuration, l.saveResult)
		l.saveResultDuration = saveResultDuration
	}

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
		panic(err)
	}
	if !reflect.DeepEqual(l.kafkaCfg, kafkaCfg) {
		l.kafkaCfg = &kafkaCfg
		if err = retry.Do(l.initConsumer, retryOpts...); err != nil {
			err = errors.WithMessage(err, "init kafka consumer failed")
			log.Fatal(err)
			panic(err)
		} else if err = retry.Do(l.initProducer, retryOpts...); err != nil {
			err = errors.WithMessage(err, "init kafka producer failed")
			log.Fatal(err)
			panic(err)
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

func (l *logParser) Run() {
	log.Info("start running...")

	ctx := context.Background()
	start := time.Now()
	for {
		msg, err := l.consumer.ReadMessage(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			continue
		}

		if len(msg.Value) == 0 {
			break
		}

		// 延时重点在io, 不需要并发优化
		l.ProcessLogMessage(msg.Value)
	}

	log.Info(time.Since(start))
	log.Info("fuck")

}

func (l *logParser) initConsumer() error {
	log.Info("init consumer")
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        l.kafkaCfg.Hosts,
		//GroupID:        l.name,
		Topic:          l.kafkaCfg.InputTopic,
		MinBytes:       l.kafkaCfg.ReadMinBytes,
		MaxBytes:       l.kafkaCfg.ReadMaxBytes,
		CommitInterval: l.kafkaCfg.CommitInterval,
		StartOffset:    kafka.FirstOffset,
	})
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
	if err = db.Table(mysqlCfg.TableName).AutoMigrate(&logTemplate{}); err != nil {
		return errors.WithMessage(err, "init db table failed")
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

func (l *logParser) saveToBuffer(msg *logTemplate) {
	id := msg.ClusterId
	if result, found := l.bufferMap[id]; found {
		result.Size++
		result.Tokens = msg.Tokens
		result.Content = msg.Content
		return
	}

	l.resultBuffer = append(l.resultBuffer, msg)
	l.bufferMap[id] = msg
}

func (l *logParser) saveResult() error {
	l.runnable.Lock()
	defer l.runnable.Unlock()

	if len(l.resultBuffer) == 0 {
		log.Info("no recent result")
		return nil
	}

	err := l.saveToDB()
	if err != nil {
		return errors.WithMessage(err, "save result to db failed")
	}

	l.resultBuffer = l.resultBuffer[:0]
	l.bufferMap = make(map[uint32]*logTemplate, len(l.bufferMap))
	return nil
}

func (l *logParser) sendRealtimeResult(template *logTemplate) {
	buf := bytes.NewBuffer([]byte{})
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
	db := l.db.Table(l.mysqlCfg.TableName)
	tx := db.Begin()
	defer tx.Commit()
	err = tx.Where("cluster_id in ?", clusterIds).Find(&oldResults).Error
	if err != nil {
		return
	}

	for _, oldTemplate := range oldResults {
		if template, found := l.bufferMap[oldTemplate.ClusterId]; found {
			template.Size += oldTemplate.Size
		}
	}

	err = tx.Clauses(
		clause.OnConflict{
			UpdateAll: true,
		},
	).Create(&l.resultBuffer).Error
	return
}
