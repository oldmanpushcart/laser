#!/bin/sh

typeset SERVER_IP=${1}
typeset SERVER_PORT=${2}
typeset RESULT_DATAFILE_PATH=${3}

java \
    -XX:+UseBiasedLocking\
    -Xmx4G \
    -Xms4G \
    -Xmn2G \
    -jar ./carrying-client/target/laser-jar-with-dependencies.jar 'client' ${SERVER_IP} ${SERVER_PORT} ${RESULT_DATAFILE_PATH} ./laser.properties

