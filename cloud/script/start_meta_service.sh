#!/usr/bin/bash

if [[ ! -d bin || ! -d conf || ! -d lib ]]; then
	echo "$0 must be invoked at the directory which contains bin, conf and lib"
	exit -1
fi

process=meta_service
if [ "$1" == "--recycler" ]; then
	process=recycler
fi

if [ -f bin/${process}.pid ]; then
	echo "pid file existed, ${process} may have already started"
	exit -1;
fi

which patchelf > /dev/null 2>&1
if [ $? -ne 0 ]; then
	echo "patchelf is needed to launch meta_service"
	exit -1
fi

lib_path=`pwd`/lib
bin=`pwd`/lib/meta_service
ldd ${bin} | grep -Ei 'libfdb_c.*not found' 2>&1 > /dev/null
if [ $? -eq 0 ]; then
	patchelf --set-rpath ${lib_path} ${bin}
	patchelf --set-interpreter ${lib_path}/ld-linux-x86-64.so.2 ${bin}
	ldd ${bin}
fi
# `$0 --recycler` to launch recycler process
nohup ${bin} "$@" > log/${process}.out 2>&1 &
echo "meta service started with args: $@"
