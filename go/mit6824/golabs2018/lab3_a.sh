#!/bin/bash

export "GOPATH=$PWD"
cd "$GOPATH/src/kvraft"
#time go test -race -run 2A
#time go test -race -run 2B
#time go test -race -run 2C
time go test -race -run 3A
#time go test -race -run TestBasic3A
#time go test -race -run TestConcurrent3A
#time go test -race -run TestManyPartitionsOneClient3A
#time go test -race -run TestManyPartitionsManyClients3A
