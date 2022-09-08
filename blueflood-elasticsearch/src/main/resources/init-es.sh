#!/bin/bash
# init-es.sh is the script to be run to properly setup the es cluster.  The main things
#  it does is set up the aliases and mappings as required by BF.  The mappings
#  are required to properly tokenize the incoming data.
# You can pass it a url if your elastic search doesn't reside on localhost:9092
#WARNING: this script will destroy existing ES data and reset it to the proper init state
# Example:
#    Local ES: ./init-es.sh
#    ES on OR: ./init-es.sh -u <url for OR:ES> -n <username> -p <password> -r <boolean to reset>

# Main features of this script that'll stop working when we upgrade Elasticsearch:
#
# 1) Mapping types are deprecated in version 7 and removed in version 8. They'll still work up to that point but may
# require you to pass a query parameter with API calls. See
# https://www.elastic.co/guide/en/elasticsearch/reference/7.17/removal-of-types.html
#
# 2) The "string" type is removed in version 5. It's replaced by two types: "text" and "keyword". See
# https://www.elastic.co/blog/strings-are-dead-long-live-strings
#
# These things are in the accompanying JSON files, not in the script itself.

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
ABSOLUTE_PATH=$(cd `dirname "$0"` && pwd)
function checkFile
{
  echo checking $1.
  if [ ! -f $ABSOLUTE_PATH/$1 ]; then
    echo $1 not found
    exit 2
  fi
}

# Exit initialization script when Blueflood is already initialized in ES and we don't need to reset it
if [ "$ES_RESET" = false ]; then
    #verify whether blueflood was already initialized by checking its marker index
    BLUEFLOOD_INITIALIZED=$(curl $AUTH  -XHEAD --write-out %{http_code} $ELASTICSEARCH_URL'/blueflood_initialized_marker')

    if [ $BLUEFLOOD_INITIALIZED = 200 ]; then
        echo "ES already initialized for Blueflood. Skipping initialization process."
        exit 0
    fi
fi

echo '# Verify config files are present'
checkFile index_settings.json
checkFile metrics_mapping.json
checkFile events_mapping.json
checkFile tokens_mapping.json

echo '# Wipe existing indexes ("not found" errors are okay)'
curl $AUTH -XDELETE $ELASTICSEARCH_URL'/blueflood_initialized_marker/'
echo ''
curl $AUTH -XDELETE $ELASTICSEARCH_URL'/events/'
echo ''
curl $AUTH -XDELETE $ELASTICSEARCH_URL'/metric_metadata/'
echo ''
curl $AUTH -XDELETE $ELASTICSEARCH_URL'/metric_metadata_v2/'
echo ''
curl $AUTH -XDELETE $ELASTICSEARCH_URL'/metric_tokens/'
echo ''

echo '# Create indexes'
curl $AUTH -XPUT $ELASTICSEARCH_URL'/metric_metadata'
echo ''
curl $AUTH -XPUT $ELASTICSEARCH_URL'/metric_tokens'
echo ''
curl $AUTH -XPUT $ELASTICSEARCH_URL'/events'
echo ''

JSON=(-H 'Content-Type: application/json')
echo '# Create aliases'
curl $AUTH "${JSON[@]}" -XPOST $ELASTICSEARCH_URL'/_aliases' -d '
{
    "actions" : [
        { "add" : { "alias" : "metric_metadata_write", "index" : "metric_metadata" } }
    ]
}'
echo ''
curl $AUTH "${JSON[@]}" -XPOST $ELASTICSEARCH_URL'/_aliases' -d '
{
    "actions" : [
        { "add" : { "alias" : "metric_metadata_read", "index" : "metric_metadata" } }
    ]
}'
echo ''
curl $AUTH "${JSON[@]}" -XPOST $ELASTICSEARCH_URL'/_aliases' -d '
{
    "actions" : [
        { "add" : { "alias" : "metric_tokens_write", "index" : "metric_tokens" } }
    ]
}'
echo ''
curl $AUTH "${JSON[@]}" -XPOST $ELASTICSEARCH_URL'/_aliases' -d '
{
    "actions" : [
        { "add" : { "alias" : "metric_tokens_read", "index" : "metric_tokens" } }
    ]
}'
echo ''

echo '# Add index settings to metric_metadata'
curl $AUTH -XPOST $ELASTICSEARCH_URL'/metric_metadata/_close'
echo ''
curl $AUTH "${JSON[@]}" -XPUT $ELASTICSEARCH_URL'/metric_metadata/_settings' -d @$ABSOLUTE_PATH/index_settings.json
echo ''
curl $AUTH -XPOST $ELASTICSEARCH_URL'/metric_metadata/_open'
echo ''

echo '# Add mappings to indexes'
curl $AUTH "${JSON[@]}" -XPUT $ELASTICSEARCH_URL'/metric_metadata/_mapping/metrics' -d @$ABSOLUTE_PATH/metrics_mapping.json
echo ''
curl $AUTH "${JSON[@]}" -XPUT $ELASTICSEARCH_URL'/metric_tokens/_mapping/tokens' -d @$ABSOLUTE_PATH/tokens_mapping.json
echo ''
curl $AUTH "${JSON[@]}" -XPUT $ELASTICSEARCH_URL'/events/_mapping/graphite_event' -d @$ABSOLUTE_PATH/events_mapping.json
echo ''

echo '# Create marker index so we know this script has run'
curl $AUTH -XPUT $ELASTICSEARCH_URL'/blueflood_initialized_marker'
echo ''
