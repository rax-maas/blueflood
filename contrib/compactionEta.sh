#/bin/bash

# Description: a script to see how much time is left on major 
#              compactions on cassandra nodes
#
# Instructions:
#
# a. To run for a single machine, replace 'domain.com' with the appropriate domain.  
#    You can change the port or leave it blank to use the default port for nodetool:
#
#       ssh db01.domain.com 'bash -s' < ./compactionEta.sh [port]
#
# b. To run for multiple machines, here is a one-liner example. This assumes you number 
#    your hosts like my team does, but hopefully you get the idea.
#
#       for x in {00..31}; do ssh db$(printf "%2.2d" $x).domain.com 'bash -s' < ./compactionEta.sh [port]; done
#
# c. Or, to run on multiple machines a little faster, install GNU parallel 
#    and run the command below. Again, replace 'domain.com' and also <start> 
#    and <end> with the first and last servers you want to check.
# 
#       parallel -j20 --no-notice "ssh db{1}.domain.com 'bash -s' < ./compactionEta.sh [port]" ::: $(seq -w <start> <end>) | sort


if [ $1 ]; then 
	PORT=$1
else
	PORT=7199
fi

# First we'll grab the seconds elapsed since a major compaction was started
SECONDS_ELAPSED=$(ps -p $(ps auxx | grep nodetool | grep compact | awk '{print $2}') -o etimes= 2>/dev/null)

# If a major compaction is running, ask nodetool how far along it is
# We will grab the largest one, assuming that it's the one we want
if [ -z $SECONDS_ELAPSED ]; then 
    echo $(hostname)": Node down or not running a major compaction"
    exit 2
else
    PERCENT_COMPLETE=$(/opt/cassandra/bin/nodetool -p $PORT compactionstats 2>/dev/null | grep 'metrics_full' | sort -n -k 5 | tail -1 | awk '{print $7}' | tr -d %)
fi

# Assuming we got $PERCENT_COMPLETE just fine, print out how much time is remaining
if [ $SECONDS_ELAPSED -a $PERCENT_COMPLETE ]; then 
    echo "$SECONDS_ELAPSED $PERCENT_COMPLETE" | awk -v HOST=$(hostname) '{printf HOST ":%8.2f hours\n", (($1*100/$2)-$1)/3600}'
else
    echo $(hostname)": Node down or not running a major compaction"
    exit 2
fi 
