#!/bin/bash

export "GOPATH=$PWD"
cd "$GOPATH/src/mapreduce"
go test -run Failure
