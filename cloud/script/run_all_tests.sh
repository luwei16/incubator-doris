#!/bin/bash

echo "$@"

# fdb memory leaks, we don't care the core dump of unit test
unset ASAN_OPTIONS

for i in `ls *_test`; do
	if [ "$1" != "" ]; then
		if [ "$1" != "${i}" ]; then
			continue;
		fi
	fi
	if [ -x ${i} ]; then
		echo "========== ${i} =========="
		fdb=$(ldd ${i} | grep libfdb_c | grep found)
		if [ "${fdb}" != "" ]; then
			patchelf --set-rpath `pwd` ${i}
			patchelf --set-interpreter `pwd`/ld-linux-x86-64.so.2 ${i}
		fi
		if [ "$2" == "" ]; then
			./${i} --gtest_print_time=true --gtest_output=xml:${i}.xml
		else
			./${i} --gtest_print_time=true --gtest_output=xml:${i}.xml --gtest_filter=$2
		fi
		echo "--------------------------"
	fi
done