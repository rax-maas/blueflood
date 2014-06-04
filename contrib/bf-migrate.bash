#!/bin/bash

CP=/Users/gdusbabek/codes/github/blueflood/blueflood-all/target/blueflood-all-2.0.0-SNAPSHOT-jar-with-dependencies.jar:/Users/gdusbabek/codes/github/blueflood/blueflood-core/target/classes

java -classpath $CP com.rackspacecloud.blueflood.tools.ops.Migration $@
