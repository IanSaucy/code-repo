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

const mydebugEnabled = false
const mylogEnabled = true

func mydebug(a ...interface{}) (n int, err error) {
	if mydebugEnabled {
		n, err = fmt.Println(a...)
	}
	return
}
func mylog(a ...interface{}) (n int, err error) {
	if mylogEnabled {
		n, err = fmt.Println(a...)
	}
	return
}
