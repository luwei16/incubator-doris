#!/bin/bash
cd `dirname $0`

process=meta_service
if [ "$1" == "--recycler" ]; then
	process=recycler
fi

if [ ! -f ${process}.pid ]; then
	echo "no ${process}.pid found, process may have been stopped"
	exit -1
fi

pid=`cat ${process}.pid`
kill ${pid}
rm -f ${process}.pid
