#!/bin/bash

export "GOPATH=$PWD"
cd "$GOPATH/src/raft"
time go test -race -run 2A
time go test -race -run 2B
time go test -race -run 2C
#time go test -race -run TestFigure82C
#time go test -race -run TestUnreliableAgree2C
#time go test -race -run TestFigure8Unreliable2C
#time go test -race -run TestUnreliableChurn2C
