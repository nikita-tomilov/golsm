#!/bin/bash
set -x
GOROOT=/usr/local/go #gosetup
GOPATH=/home/hotaro/go #gosetup
echo Launching tests
go test -v ./...
