#!/bin/bash

script_path=$(cd "$(dirname "$0")"; pwd)
lib_path=${script_path}/../lib
main_class="com.scistor.process.thrift.server.StartControlServer"
conf_path=${script_path}/../conf

CLASSPATH="${conf_path}"
for jar in $lib_path/*.jar
do
	if [ "$CLASSPATH" = "" ] ; then
		CLASSPATH=$jar
	else
		CLASSPATH=$CLASSPATH:$jar
	fi
done

export CLASSPATH=$CLASSPATH
java -jar ${script_path}/../MasterServer-1.0-SNAPSHOT.jar
