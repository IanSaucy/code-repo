#!/bin/bash

export "GOPATH=$PWD"
cd "$GOPATH/src/raft"
time go test -race -run 2A
time go test -race -run 2B
