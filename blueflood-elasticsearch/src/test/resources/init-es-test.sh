#!/bin/bash
# This is a test script to certain Elasticsearch indices

# Set default ES URL
ELASTICSEARCH_URL=http://localhost:9200

usage() {
    echo "Usage: $0 [-u <remote ES url:string>(default: localhost:9200)] [-n <username:string>] [-p <password:string>] [-r <reset:boolean>(default: true)]" 1>&2;
    exit 1
}

while getopts "p:u:n:r:" o; do
    case "${o}" in
        p) ES_PASSWD=$OPTARG ;;
        u) ELASTICSEARCH_URL=$OPTARG ;;
        n) ES_USERNAME=$OPTARG ;;
        r) ES_RESET=$OPTARG ;;
        *) usage ;;
    esac
done

# Set a auth header for curl if username and passwd is supplied
# Allows initing ES when ES is supplied by a SaaS provider like ObjectRocket
if [ $ES_USERNAME ] && [ $ES_PASSWD ]; then
    AUTH="-u ${ES_USERNAME}:${ES_PASSWD}"
fi

# Using this in the checkfile function allows us to run this script from any directory
ABSOLUTE_PATH=$(cd `dirname "${BASH_SOURCE[0]}"` && pwd)
function checkFile
{
  echo checking $1.
  if [ ! -f $ABSOLUTE_PATH/$1 ]; then
    echo $1 not found
    exit 2
  fi
}

checkFile tokens_mapping.json

#delete the old indices
curl $AUTH -XDELETE $ELASTICSEARCH_URL'/metric_tokens_v1/' >& /dev/null

#create the new indices
curl $AUTH -XPUT $ELASTICSEARCH_URL'/metric_tokens_v1'

#add mappings to metric_tokens index
curl $AUTH -XDELETE $ELASTICSEARCH_URL'/metric_tokens_v1/_mapping/tokens' >& /dev/null
curl $AUTH -XPUT $ELASTICSEARCH_URL'/metric_tokens_v1/_mapping/tokens' -d @tokens_mapping.json