#!/bin/bash

export "GOPATH=$PWD"
cd "$GOPATH/src/raft"
time go test -race -run 2A
time go test -race -run 2B
#time go test -race -run TestBasicAgree2B
#time go test -race -run TestFailAgree2B
#time go test -race -run TestFailNoAgree2B
#time go test -race -run TestConcurrentStarts2B
#time go test -race -run TestRejoin2B
#time go test -race -run TestBackup2B
#time go test -race -run TestCount2B
