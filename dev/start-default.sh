#!/bin/bash
#set -x
echo '-------------------------------------------------- dataCenter-mlsql-engine application --------------------------------------------------------'

ROOT_PATH=$(cd $(dirname $0); pwd)
PID_PATH=${ROOT_PATH}'/pid'
LOG_PATH=${ROOT_PATH}'/logs'
LOG_OUTPUT_PATH=${LOG_PATH}'/mlsql-engine-execute.out'

if [[ -z "${SPARK_HOME}" ]]; then
    echo "===SPARK_HOME is required==="
    exit 1
fi

SELF=$(cd $(dirname $0) && pwd)
cd $SELF


PIDS=$(ps -ef | grep spark-submit | grep -v 'grep' | awk '{print $2}')
for pid in $PIDS ; do
    echo $pid
    kill -9 $pid
done

if [[ ! -f ${PID_PATH} ]]; then
     touch ${PID_PATH}
     echo 'pid file create success'
    else
    if [ $PID_EXIST ];then
         #if [[ -s ${PID_PATH} ]]; then
         echo 'stop mlsql-engine application at pid : '$(cat ${PID_PATH})
         kill -9 $(cat ${PID_PATH})
        else
         echo 'mlsql-engine application is not running!'
     fi
fi

if [[ ! -d ${LOG_PATH} ]]; then
    mkdir ${LOG_PATH}
fi
echo '' > ${LOG_OUTPUT_PATH}
chmod -R 755 ./*

nohup  ./start-local.sh > ${LOG_OUTPUT_PATH}  2>&1 &

echo $! > ${PID_PATH}
echo 'mlsql-engine at pid : '$(cat ${PID_PATH})