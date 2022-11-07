#!/bin/bash

if [ $# != 2 ]
then
echo "usage: $0 backupdir waitsecond"
exit 1
fi

BACKUPDIR=$1
LOGDIR=$1
WAITSEC=$2
INTERVALSEC=5

CURRENTDATE=$(date +%Y-%m-%d-%H-%M)

echo "start backup:" $(date +%Y-%m-%d-%H-%M-%S) >> ${LOGDIR}/backup.log

begin_time=$(date +%s)
BACKUPCMD=`fdbbackup start -d file://${BACKUPDIR} -t ${CURRENTDATE}`
echo $BACKUPCMD

echo "$BACKUPCMD" >> ${LOGDIR}/backup.log

i=0
succ=false
while ((i < ${WAITSEC}))
do
    sleep ${INTERVALSEC}
    ((i += ${INTERVALSEC} ))
    check_results=`fdbbackup status -t ${CURRENTDATE} | grep completed`
    echo "backup check result: $check_results"
    if [[ $check_results =~ "completed" ]]
    then
        end_time=$(date +%s)
        cost_s=`expr ${end_time} - ${begin_time}`
        echo "backup succ, cost:" ${cost_s}"s" >> ${LOGDIR}/backup.log
        kv_size=`fdbcli --exec "status" | grep "Sum of key-value sizes" | sed "s/^[ \t]*//g"`
        echo "$kv_size" >> ${LOGDIR}/backup.log
        succ=true
        break
    fi
done

if [[ ${succ} == "false" ]]
then
    echo "backup fail" >> ${LOGDIR}/backup.log
    abort_result=`fdbbackup abort -t ${CURRENTDATE}`
    echo "abort result: $abort_result" >> ${LOGDIR}/backup.log
else
    delete_old_file=`cd ${BACKUPDIR} && ls -ltr | grep '^d' | head -1 | grep -v ${CURRENTDATE} | xargs | awk '{print $9}'`
    if [[ $delete_old_file =~ "backup-" ]]
    then
        echo "delete ${delete_old_file}" >> ${LOGDIR}/backup.log
        delete_result=`fdbbackup delete -d file://${BACKUPDIR}/${delete_old_file}`
        echo "delete result: $delete_result" >> ${LOGDIR}/backup.log
    fi
fi