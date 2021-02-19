package template_miner

import (
	"bufio"
	"bytes"
	"compress/gzip"
	jsoniter "github.com/json-iterator/go"
	"github.com/pkg/errors"
	"github.com/prometheus/common/log"
	"github.com/spf13/viper"
	"github.com/valyala/bytebufferpool"
	"runtime"
	"strings"
	"sync"
	"test/log_parser/template_miner/drain"
	"test/util"
	"time"
)

const defaultLevel = "default" // 默认路由到default级别

type TemplateMiner struct {
	// 按日志级别路由
	levelDrains map[string]*drain.Drain
	masker      *RegexMasker

	compressState      bool          // 持久化时是否启用压缩
	snapshotDuration   time.Duration // 持久化间隔
	persistenceHandler PersistenceHandler

	saveStateDone chan<- struct{} // 停止持久化ticker

	lock     sync.Mutex   // 保证底层的drain线程安全
	runnable sync.RWMutex // 重载配置时阻塞新请求
}

func New(config *viper.Viper, persistenceHandlerFun PersistenceHandlerFun) (*TemplateMiner, error) {
	templateMiner := new(TemplateMiner)
	err := templateMiner.ReloadFromConfig(config, persistenceHandlerFun)
	if err != nil {
		return nil, errors.WithMessage(err, "reload template miner config failed")
	}
	runtime.SetFinalizer(templateMiner, templateMinerDestructor)

	return templateMiner, nil
}

// 模拟析构函数 关闭ticker 防止内存泄漏
func templateMinerDestructor(t *TemplateMiner) {
	if t.saveStateDone != nil {
		close(t.saveStateDone)
	}
}

func (t *TemplateMiner) ReloadFromConfig(config *viper.Viper, newPersistenceHandler PersistenceHandlerFun) error {
	t.runnable.Lock()
	defer t.runnable.Unlock()

	newLevels := config.GetStringSlice("levels")
	if len(newLevels) == 0 {
		newLevels = []string{defaultLevel} // 默认含有defaultLevel
	}
	newLevelsSet := util.StringSlice2Set(newLevels)
	if t.levelDrains == nil {
		t.levelDrains = make(map[string]*drain.Drain, len(newLevels))
	}

	// 剔除在新配置中被删除的level
	for level := range t.levelDrains {
		if _, found := newLevelsSet[level]; !found {
			delete(t.levelDrains, level)
		}
	}

	for _, level := range newLevels {
		// 添加新的level
		if _, found := t.levelDrains[level]; !found {
			t.levelDrains[level] = drain.New()
		}

		// 重载drain的配置
		t.levelDrains[level].SetMetaData(
			config.GetInt("max_depth"),
			config.GetInt("max_children"),
			config.GetFloat64("similarity_threshold"),
			config.GetStringSlice("extra_delimiters"),
		)
	}

	// 获取正则
	maskingStr := config.GetString("masking")
	var err error
	t.masker, err = NewRegexMasker([]byte(maskingStr))
	if err != nil {
		return errors.WithMessage(err, "compile regexp failed")
	}
	t.compressState = config.GetBool("compress_state")

	// 获取持久化handler
	if newPersistenceHandler != nil {
		t.persistenceHandler, err = newPersistenceHandler(config)
		if err != nil {
			return errors.WithMessage(err, "init persistence handler failed")
		}
	}

	snapshotDuration := config.GetDuration("snapshot_duration")
	if snapshotDuration != t.snapshotDuration {
		templateMinerDestructor(t)
		// 启用定时函数
		t.saveStateDone = util.SetInterval(snapshotDuration, t.SaveState)
		t.snapshotDuration = snapshotDuration
	}

	return nil
}

func (t *TemplateMiner) ProcessLogMessage(content, level string) (*drain.LogCluster, string) {
	t.runnable.RLock()
	defer t.runnable.RUnlock()

	content = strings.TrimSpace(content)
	// 根据预先定义的正则去除干扰信息
	maskedContent := t.masker.Mask(content)
	level = strings.ToLower(level)

	if _, found := t.levelDrains[level]; !found {
		level = defaultLevel
	}
	drain := t.levelDrains[level]

	//  t.masker.Mask占用90%以上的耗时，drain算法无需并发处理
	t.lock.Lock()
	defer t.lock.Unlock()

	cluster := drain.ProcessLogMessage(maskedContent)
	return cluster, level
}

func (t *TemplateMiner) SaveState() error {
	// 编码状态信息
	t.lock.Lock()
	buf := bytebufferpool.Get()
	defer bytebufferpool.Put(buf)

	enc := jsoniter.NewEncoder(buf)
	err := enc.Encode(t.levelDrains)
	t.lock.Unlock()
	if err != nil {
		return errors.WithMessage(err, "marshal state failed")
	}

	var state []byte
	// gzip压缩
	if t.compressState {
		compressedStateBuf := bytebufferpool.Get()
		defer bytebufferpool.Put(compressedStateBuf)

		w := gzip.NewWriter(compressedStateBuf)
		_, err := buf.WriteTo(w)
		if err != nil {
			return errors.WithMessage(err, "gzip state failed")
		}
		// 坑，不能defer调用gzipWriter.Close(), 此函数还会写入部分数据
		err = w.Close()
		if err != nil {
			return errors.WithMessage(err, "close gzip writer failed")

		}

		state = compressedStateBuf.Bytes()
	} else {
		state = buf.Bytes()
	}

	err = t.persistenceHandler.SaveState(state)
	if err != nil {
		return errors.WithMessage(err, "save state failed")
	}
	return nil
}

func (t *TemplateMiner) LoadState() error {
	state, err := t.persistenceHandler.LoadState()
	if err != nil {
		return err
	}

	// gzip解压
	if t.compressState {
		r, err := gzip.NewReader(bytes.NewReader(state))
		if err != nil {
			return errors.WithMessage(err, "create gzip reader failed")
		}
		defer r.Close()

		uncompressedState := bytebufferpool.Get()
		defer bytebufferpool.Put(uncompressedState)

		w := bufio.NewWriter(uncompressedState)
		_, err = w.ReadFrom(r)
		if err != nil {
			return errors.WithMessage(err, "gzip state failed")
		}

		err = w.Flush()
		if err != nil {
			return errors.WithMessage(err, "gzip flush failed")
		}

		state = uncompressedState.Bytes()
	}

	savedDrains := make(map[string]*drain.Drain)
	err = jsoniter.Unmarshal(state, &savedDrains)
	if err != nil {
		return errors.WithMessage(err, "unmarshal state failed")
	}

	for level, savedDrain := range savedDrains {
		if drain := t.levelDrains[level]; drain != nil {
			drain.LoadOldData(savedDrain)
		}
	}

	log.Info("load state success")
	return nil
}
