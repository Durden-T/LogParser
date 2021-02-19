package template_miner

import (
	"github.com/spf13/viper"
	"io/ioutil"
)

// 持久化接口
type PersistenceHandler interface {
	SaveState(state []byte) error
	LoadState() ([]byte, error)
}

// 获取持久化handler的函数
type PersistenceHandlerFun = func(*viper.Viper) (PersistenceHandler, error)

type FilePersistenceHandler struct {
	FilePath string
}

func (h *FilePersistenceHandler) SaveState(state []byte) error {
	return ioutil.WriteFile(h.FilePath, state, 0666)
}

func (h *FilePersistenceHandler) LoadState() ([]byte, error) {
	return ioutil.ReadFile(h.FilePath)
}

func NewFilePersistenceHandler(cfg *viper.Viper) (PersistenceHandler, error) {
	filePath := cfg.GetString("persistenceFilePath")
	if filePath == "" {
		filePath = "save.txt"
	}
	return &FilePersistenceHandler{FilePath: filePath}, nil
}
