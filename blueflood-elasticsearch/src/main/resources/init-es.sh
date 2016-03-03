#!/bin/bash
# init-es.sh is the script to be run to properly setup the es cluster.  The main things
#  it does is set up the aliases and mappings as required by BF.  The mappings
#  are required to properly tokenize the incoming data.
# You can pass it a url if your elastic search doesn't reside on localhost:9092
#WARNING: this script will destroy existing ES data and reset it to the proper init state
# Example:
#    Local ES: ./init-es.sh
#    ES on OR: ./init-es.sh -u <url for OR:ES> -n <username> -p <password>

# Set default ES URL
ELASTICSEARCH_URL=http://localhost:9200

usage() {
    echo "Usage: $0 [-u <remote ES url:string>(default: localhost:9200)] [-n <username:string>] [-p <password:string>]" 1>&2; 
    exit 1
}

while getopts "p:u:n:" o; do
    case "${o}" in
        p) ES_PASSWD=$OPTARG ;;
        u) ELASTICSEARCH_URL=$OPTARG ;;
        n) ES_USERNAME=$OPTARG ;;
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

checkFile index_settings.json
checkFile metrics_mapping.json
checkFile metrics_mapping_enums.json
checkFile events_mapping.json

#delete the old indices
curl $AUTH -XDELETE $ELASTICSEARCH_URL'/enums/' >& /dev/null
curl $AUTH -XDELETE $ELASTICSEARCH_URL'/events/' >& /dev/null
curl $AUTH -XDELETE $ELASTICSEARCH_URL'/metric_metadata/' >& /dev/null
curl $AUTH -XDELETE $ELASTICSEARCH_URL'/metric_metadata_v2/' >& /dev/null
curl $AUTH -XDELETE $ELASTICSEARCH_URL'/metric_metadata_write/' >& /dev/null


#create the new indices
curl $AUTH -XPUT $ELASTICSEARCH_URL'/metric_metadata'
curl $AUTH -XPUT $ELASTICSEARCH_URL'/enums'
curl $AUTH -XPUT $ELASTICSEARCH_URL'/events'

#create the aliases
curl $AUTH -XPOST $ELASTICSEARCH_URL'/_aliases' -d '
{
    "actions" : [
        { "add" : { "alias" : "metric_metadata_write", "index" : "metric_metadata" } }
    ]
}'
curl $AUTH -XPOST $ELASTICSEARCH_URL'/_aliases' -d '
{
    "actions" : [
        { "add" : { "alias" : "metric_metadata_read", "index" : "metric_metadata" } }
    ]
}'
curl $AUTH -XPOST $ELASTICSEARCH_URL'/_aliases' -d '
{
    "actions" : [
        { "add" : { "alias" : "metric_metadata_read", "index" : "enums" } }
    ]
}'

#add index settings to metric_metadata index
curl $AUTH -XPOST $ELASTICSEARCH_URL'/metric_metadata/_close'
curl $AUTH -XPUT $ELASTICSEARCH_URL'/metric_metadata/_settings' -d @index_settings.json
curl $AUTH -XPOST $ELASTICSEARCH_URL'/metric_metadata/_open'


#add mappings to metric_metadata index
curl $AUTH -XDELETE $ELASTICSEARCH_URL'/metric_metadata/_mapping/metrics' >& /dev/null
curl $AUTH -XPUT $ELASTICSEARCH_URL'/metric_metadata/_mapping/metrics' -d @metrics_mapping.json

#add index settings to enums index
curl $AUTH -XPOST $ELASTICSEARCH_URL'/enums/_close'
curl $AUTH -XPUT $ELASTICSEARCH_URL'/enums/_settings' -d  @index_settings.json
curl $AUTH -XPOST $ELASTICSEARCH_URL'/enums/_open'

#add mappings to enums index
curl $AUTH -XDELETE $ELASTICSEARCH_URL'/enums/_mapping/metrics'  >& /dev/null
curl $AUTH -XPUT $ELASTICSEARCH_URL'/enums/_mapping/metrics' -d @metrics_mapping_enums.json

#add mappings to graphite_event index
curl $AUTH -XDELETE $ELASTICSEARCH_URL'/events/_mapping/graphite_event' >& /dev/null
curl $AUTH -XPUT $ELASTICSEARCH_URL'/events/_mapping/graphite_event' -d @events_mapping.json

