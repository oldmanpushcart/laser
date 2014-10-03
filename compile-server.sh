#!/bin/sh

svn up
mvn package -Dmaven.test.skip=true
