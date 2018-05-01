#!/bin/bash

export "GOPATH=$PWD"
cd "$GOPATH/src/main"
bash ./test-mr.sh
