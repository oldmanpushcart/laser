#!/bin/bash

# args[1] is launch's type
# args[2] is server's dataFile path
# args[3] is server's port

typeset DATAFILE_PATH=${1}
typeset SERVER_PORT=${2}


java \
    -XX:+UseBiasedLocking \
    -Xmx4G \
    -Xms4G \
    -Xmn2G \
    -XX:+UseConcMarkSweepGC \
    -XX:CMSMaxAbortablePrecleanTime=5000 \
    -XX:+CMSClassUnloadingEnabled \
    -XX:CMSInitiatingOccupancyFraction=80 \
    -XX:+UseCMSInitiatingOccupancyOnly \
    -XX:ParallelGCThreads=12 \
    -jar ./target/laser-jar-with-dependencies.jar 'server' ${DATAFILE_PATH} ${SERVER_PORT} ./laser.properties