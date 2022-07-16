#!/bin/bash

THIS_DIR="$(readlink -f $(dirname $0))"

sudo docker run -it --rm \
		-e HADOOP_CLASSPATH=/code/target/hlab-1.0-SNAPSHOT.jar \
		-v $THIS_DIR:/code/ \
				--name hadoop-standalone-$(date +%s) \
                zhuzhenle/hadoop-standalone bash
