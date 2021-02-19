package util

import (
	"github.com/prometheus/common/log"
	"time"
	"unsafe"
)

type StringSet = map[string]struct{}

func StringSlice2Set(slice []string) StringSet {
	set := make(StringSet, len(slice))
	for _, val := range slice {
		set[val] = struct{}{}
	}
	return set
}

func StrToBytes(s string) (b []byte) {
	*(*string)(unsafe.Pointer(&b)) = s                                                  // 把s的地址付给b
	*(*int)(unsafe.Pointer(uintptr(unsafe.Pointer(&b)) + 2*unsafe.Sizeof(&b))) = len(s) // 修改容量为长度
	return
}

// []byte转string
func BytesToStr(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

func SetInterval(duration time.Duration, f func() error) chan<- struct{} {
	done := make(chan struct{}, 1)
	go func() {
		timer := time.NewTicker(duration)
		defer timer.Stop()
		for {
			select {
			case <-timer.C:
				err := f()
				if err != nil {
					log.Error(err)
				}
			case <-done:
				return
			}
		}
	}()
	return done
}
