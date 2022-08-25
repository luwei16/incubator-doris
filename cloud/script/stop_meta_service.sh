#!/bin/bash
cd `dirname $0`
pid=`cat meta_service.pid`
kill ${pid}
rm meta_service.pid
