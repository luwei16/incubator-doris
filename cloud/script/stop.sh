#!/usr/bin/bash
curdir="$(cd "$(dirname $(readlink -f ${BASH_SOURCE[0]}))" &>/dev/null && pwd)"
selectdb_home="$(
    cd "${curdir}/.."
    pwd
)"

cd ${selectdb_home}

process=selectdb_cloud

if [ ! -f ${selectdb_home}/bin/${process}.pid ]; then
	echo "no ${process}.pid found, process may have been stopped"
	exit -1
fi

pid=`cat ${selectdb_home}/bin/${process}.pid`
kill -2 ${pid}
rm -f ${selectdb_home}/bin/${process}.pid
