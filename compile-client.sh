#!/bin/sh
git pull
mvn package -Dmaven.test.skip=true
