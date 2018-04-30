#!/bin/bash

export "GOPATH=$PWD"
cd "$GOPATH/src/main"
# go run wc.go master sequential pg-*.txt
# sort -n -k2 mrtmp.wcseq | tail -10
bash ./test-wc.sh
