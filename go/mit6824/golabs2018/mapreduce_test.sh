#!/bin/bash

export "GOPATH=$PWD"
cd "$GOPATH/src/mapreduce"
# 把 common.go 中 debugEnabled = true
# go test -v -run Sequential
go test -run Sequential
