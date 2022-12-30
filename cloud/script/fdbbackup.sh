#!/bin/bash

if [ $# != 3 ]
then
echo "usage: $0 <backupdir> <backup_waitsecond> <backup_keep_days>, e.g., ./fdbbackup.sh /mnt/backup 3600 15"
exit 1
fi

BACKUPDIR=$1
LOGDIR=$1
WAITSEC=$2
KEEYDAYS=$3
INTERVALSEC=5

CURRENTDATE=$(date +%Y-%m-%d)

echo "start backup:" $(date +%Y-%m-%d-%H-%M-%S) >> ${LOGDIR}/backup.log

begin_time=$(date +%s)
BACKUPCMD=`fdbbackup start -d file://${BACKUPDIR} -t ${CURRENTDATE}`
echo $BACKUPCMD

echo "$BACKUPCMD" >> ${LOGDIR}/backup.log

# start to backup
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

# if failed to backup, abort it
if [[ ${succ} == "false" ]]
then
    echo "backup fail" >> ${LOGDIR}/backup.log
    abort_result=`fdbbackup abort -t ${CURRENTDATE}`
    echo "abort result: $abort_result" >> ${LOGDIR}/backup.log
fi

# delete outdated backup data
echo 'delete outdated backup data' >> ${LOGDIR}/backup.log
delete_list=`find ${LOGDIR}/ -type d -name "backup-*" -mtime +${KEEYDAYS}`
echo "delete list: ${delete_list}" >> ${LOGDIR}/backup.log 
find ${LOGDIR}/ -type d -name "backup-*" -mtime +${KEEYDAYS} -prune -exec rm -rf {} +