#!/bin/bash
set -x
echo Launching tests
go test -v -count=1 ./...
