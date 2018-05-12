#!/bin/bash

export "GOPATH=$PWD"
cd "$GOPATH/src/raft"
go test -race -run 2A
