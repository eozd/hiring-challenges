#!/usr/bin/env bash

mvn exec:java -Dexec.mainClass="com.eozd.KafkaApp" -Dexec.args="$@"
