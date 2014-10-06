#!/bin/sh

typeset SERVER_IP=${1}
typeset SERVER_PORT=${2}
typeset RESULT_DATAFILE_PATH=${3}

java \
    -XX:+UseBiasedLocking\
    -Xmx8G \
    -Xms8G \
    -Xmn4G \
    -XX:+UseConcMarkSweepGC \
    -XX:CMSMaxAbortablePrecleanTime=5000 \
    -XX:+CMSClassUnloadingEnabled \
    -XX:CMSInitiatingOccupancyFraction=80 \
    -XX:+UseCMSInitiatingOccupancyOnly \
    -XX:CompileThreshold=1500 \
    -XX:-PrintCompilation \
    -XX:PretenureSizeThreshold=1048576 \
    -XX:+UseThreadPriorities \
    -XX:-RestrictContended \
    -jar ./target/laser-jar-with-dependencies.jar 'nioclient' ${SERVER_IP} ${SERVER_PORT} ${RESULT_DATAFILE_PATH} ./laser.properties

