package raft

import "log"
import "fmt"

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

const mylogEnabled = false
const mylogEnabled2 = true

func mylog(a ...interface{}) (n int, err error) {
	if mylogEnabled {
		n, err = fmt.Println(a...)
	}
	return
}
func mylog2(a ...interface{}) (n int, err error) {
	if mylogEnabled2 {
		n, err = fmt.Println(a...)
	}
	return
}
