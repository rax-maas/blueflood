#!/bin/bash
#
# Checks whether a Cassandra repair is still running

# Usage info
show_help() {
cat << EOF

Description:    This script shows the current status of a repair running on a Cassandra node.
                Elasticsearch is required so that we can query efficiently.

                Note that NODE is required, and it should be any part of the hostname
                of the node that will uniquely identify the node.


Usage:          ${0##*/} -n NODE -e ES_HOST [-d DAYS] 
    
    -h or -?        Display this help and exit.

    -v              Display verbose information.
    
    -n NODE         Which node to check. This can be any portion of the hostname
                    that uniquely identifies the node. (required)
    
    -e ES_HOST      Hostname for Elasticsearch (required) 
    
    -d DAYS         How many days of logs to search. If the script fails to return 
                    data, try increasing the default value here. (Default: 3)
                    

EOF
}                

# Initialize  variables:
DAYS="3"


# Process command line arguments:
OPTIND=1
while getopts "h?vn:d:e:" opt; do
    case "$opt" in
        h|\?)
            show_help
            exit 0
            ;;
        v)  DEBUG=1 
            ;;
        n)  NODE=$OPTARG
            ;;
        e)  ES_HOST=$OPTARG
            ;;
        d)  DAYS=$OPTARG
            ;;
    esac
done

if [ -z $NODE -o -z $ES_HOST ]; then 
    echo "Node and a Hostname for Elasticsearch is required"
    exit 2 
fi 

shift "$((OPTIND-1))" # Shift off the options and optional --.

NOW=$(date +%s000)
START=$(date -v-${DAYS}d +%s000)

TOTAL_SUCCESSFUL_RANGE_REPAIRS=$(curl -s -XGET 'http://'$ES_HOST'/_count' -d '{"query":{"filtered":{"query":{"bool":{"should":[{"query_string":{"query":"level:\"info\" AND (*)"}}]}},"filter":{"bool":{"must":[{"range":{"@timestamp":{"from":'$START',"to":'$NOW'}}},{"fquery":{"query":{"query_string":{"query":"host:(\"'${NODE}'\")"}},"_cache":true}},{"fquery":{"query":{"query_string":{"query":"method:(\"syncComplete\")"}},"_cache":true}},{"fquery":{"query":{"query_string":{"query":"message:(\"is fully synced\")"}},"_cache":true}}]}}}}}'|  grep -oP 'count\"\:\K[[:digit:]]+(?=,)')
TOTAL_TO_REPAIR=$(curl -s -XGET 'http://'$ES_HOST'/_search' -d '{"fields":"message","query":{"filtered":{"query":{"bool":{"should":[{"query_string":{"query":"message: \"Starting repair\""}}]}},"filter":{"bool":{"must":[{"range":{"@timestamp":{"from":'$START',"to":'$NOW'}}},{"fquery":{"query":{"query_string":{"query":"host: '${NODE}'"}},"_cache":true}}]}}}}}'|  grep -oP '\s\K[[:digit:]]+(?=\s)')



echo "# successful repairs: $TOTAL_SUCCESSFUL_RANGE_REPAIRS"
echo "# total ranges to repair: $TOTAL_TO_REPAIR"
echo Percent complete: $( echo "scale=2;$TOTAL_SUCCESSFUL_RANGE_REPAIRS*100/$TOTAL_TO_REPAIR" | bc )
