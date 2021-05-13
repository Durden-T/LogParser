package main

import (
	//_ "net/http/pprof"
	"test/log_parser"
)

const configPath = "config.yml"

func main() {
	logParser, _ := log_parser.New(configPath)
	logParser.Run()
}
