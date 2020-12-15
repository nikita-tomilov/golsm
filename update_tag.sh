#!/bin/bash
VER=v0.0.3
git tag -d $VER
git push --delete origin $VER
git tag $VER
git push --tags
