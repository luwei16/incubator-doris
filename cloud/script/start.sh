#!/usr/bin/bash

if [[ ! -d bin || ! -d conf || ! -d lib ]]; then
	echo "$0 must be invoked at the directory which contains bin, conf and lib"
	exit -1
fi

daemonized=0
for arg do
  shift
  [ "$arg" = "--daemonized" ] && daemonized=1 && continue
  [ "$arg" = "-daemonized" ] && daemonized=1 && continue
  set -- "$@" "$arg"
done
# echo "$@" "daemonized=${daemonized}"}

process=selectdb_cloud

if [ -f bin/${process}.pid ]; then
	echo "pid file existed, ${process} may have already started"
	exit -1;
fi

if ! command -v patchelf &> /dev/null; then
	echo "patchelf is needed to launch meta_service"
	exit -1
fi

lib_path=$(pwd)/lib
bin=$(pwd)/lib/selectdb_cloud
ldd ${bin} | grep -Ei 'libfdb_c.*not found' &> /dev/null
if [ $? -eq 0 ]; then
	patchelf --set-rpath ${lib_path} ${bin}
	ldd ${bin}
fi

mkdir -p log

echo "starts ${process} with args: $@"
if [ ${daemonized} -eq 1 ]; then
    nohup ${bin} "$@" > log/${process}.out 2>&1 &
else
    ${bin} "$@"
fi
