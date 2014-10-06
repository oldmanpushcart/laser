#!/bin/sh

typeset SERVER_IP=${1}
typeset SERVER_PORT=${2}
typeset RESULT_DATAFILE_PATH=${3}

java \
    -XX:+UseBiasedLocking\
    -Xmx8G \
    -Xms8G \
    -Xmn4G \
    -Xnoclassgc \
    -XX:CompileThreshold=1500 \
    -XX:-PrintCompilation \
    -XX:PretenureSizeThreshold=1048576 \
    -jar ./target/laser-jar-with-dependencies.jar 'nioclient' ${SERVER_IP} ${SERVER_PORT} ${RESULT_DATAFILE_PATH} ./laser.properties

