package main

import (
	//_ "net/http/pprof"
	"test/log_parser"
)

const configPath = "config.yml"

func main() {
	logParser, _ := log_parser.New(configPath)
	//go func() {
	//	http.ListenAndServe("0.0.0.0:9999", nil)
	//}()
	logParser.Run()
}
