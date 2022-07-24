#!/bin/bash

for i in `ls *_test`; do
	if [ -x ${i} ]; then
		./${i} --gtest_print_time=true --gtest_output=xml:${i}.xml $@
	fi
done
