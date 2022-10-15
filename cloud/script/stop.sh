#!/bin/bash
cd `dirname $0`

process=selectdb_cloud

if [ ! -f ${process}.pid ]; then
	echo "no ${process}.pid found, process may have been stopped"
	exit -1
fi

pid=`cat ${process}.pid`
kill -2 ${pid}
rm -f ${process}.pid
