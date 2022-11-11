#!/usr/bin/bash
curdir="$(cd "$(dirname $(readlink -f ${BASH_SOURCE[0]}))" &>/dev/null && pwd)"
selectdb_home="$(
    cd "${curdir}/.."
    pwd
)"

cd ${selectdb_home}

if [[ ! -d bin || ! -d conf || ! -d lib ]]; then
	echo "$0 must be invoked at the directory which contains bin, conf and lib"
	exit -1
fi

daemonized=0
for arg do
  shift
  [ "$arg" = "--daemonized" ] && daemonized=1 && continue
  [ "$arg" = "-daemonized" ] && daemonized=1 && continue
  [ "$arg" = "--daemon" ] && daemonized=1 && continue
  set -- "$@" "$arg"
done
# echo "$@" "daemonized=${daemonized}"}

process=selectdb_cloud

if [ -f ${selectdb_home}/bin/${process}.pid ]; then
	echo "pid file existed, ${process} may have already started"
	exit -1;
fi

lib_path=${selectdb_home}/lib
bin=${selectdb_home}/lib/selectdb_cloud
ldd ${bin} | grep -Ei 'libfdb_c.*not found' &> /dev/null
if [ $? -eq 0 ]; then
    if ! command -v patchelf &> /dev/null; then
        echo "patchelf is needed to launch meta_service"
        exit -1
    fi
	patchelf --set-rpath ${lib_path} ${bin}
	ldd ${bin}
fi

mkdir -p ${selectdb_home}/log
echo "starts ${process} with args: $@"
if [ ${daemonized} -eq 1 ]; then
    nohup ${bin} "$@" > ${selectdb_home}/log/${process}.out 2>&1 &
else
    ${bin} "$@"
fi
