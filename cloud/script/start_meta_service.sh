#!/usr/bin/bash

if [[ ! -d bin || ! -d conf || ! -d lib ]]; then
	echo "$0 must be invoke at the same dir as bin, conf and lib"
	exit -1
fi

which patchelf > /dev/null 2>&1
if [ $? -ne 0 ]; then
	echo "patchelf is needed to launch meta_service"
	exit -1
fi

lib_path=`pwd`/lib
bin=`pwd`/lib/meta_service
patchelf --set-rpath ${lib_path} ${bin}
patchelf --set-interpreter ${lib_path}/ld-linux-x86-64.so.2 ${bin}
ldd ${bin}
${bin}
