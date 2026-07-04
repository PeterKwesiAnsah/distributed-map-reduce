package main

//
// start a worker process, which is implemented
// in ../mr/worker.go. typically there will be
// multiple worker processes, talking to one coordinator.
//
// go run mrworker.go wc.so
//
// Please do not change this file.
//

import (
	"distributed-map-reduce/mr"
	"fmt"
	"os"
)

func main() {
	if len(os.Args) != 3 {
		fmt.Fprintf(os.Stderr, "Usage: mrworker xxx.so sockname\n")
		os.Exit(1)
	}
	mapf, reducef := loadPlugin(os.Args[1])
	mr.Worker(os.Args[2], mapf, reducef)
}
