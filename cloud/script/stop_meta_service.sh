#!/bin/bash
cd `dirname $0`

if [ ! -f meta_service.pid ]; then
	echo "no meta_service.pid found, process may have been stopped"
	exit -1
fi

pid=`cat meta_service.pid`
kill ${pid}
rm -f meta_service.pid
