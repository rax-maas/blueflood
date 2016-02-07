#!/bin/bash
# init-es.sh is the script to be run to properly setup the es cluster.  The main things
#  it does is set up the aliases and mappings as required by BF.  The mappings
#  are required to properly tokenize the incoming data.
# You can pass it a url if your elastic search doesn't reside on localhost:9092
#WARNING: this script will destroy existing ES data and reset it to the proper init state

ELASTICSEARCH_URL=${1:-"http://localhost:9200"}

function checkFile
{
  echo checking $1.
  if [ ! -f $1 ]; then 
    echo $1 not found
    exit 2
  fi
}

checkFile index_settings.json
checkFile metrics_mapping.json
checkFile metrics_mapping_enums.json
checkFile events_mapping.json

#delete the old indices
curl -XDELETE $ELASTICSEARCH_URL'/enums/' >& /dev/null
curl -XDELETE $ELASTICSEARCH_URL'/events/' >& /dev/null
curl -XDELETE $ELASTICSEARCH_URL'/metric_metadata/' >& /dev/null
curl -XDELETE $ELASTICSEARCH_URL'/metric_metadata_v2/' >& /dev/null
curl -XDELETE $ELASTICSEARCH_URL'/metric_metadata_write/' >& /dev/null


#create the new indices
curl -XPUT $ELASTICSEARCH_URL'/metric_metadata'
curl -XPUT $ELASTICSEARCH_URL'/enums'
curl -XPUT $ELASTICSEARCH_URL'/events'

#create the aliases
curl -XPOST $ELASTICSEARCH_URL'/_aliases' -d '
{
    "actions" : [
        { "add" : { "alias" : "metric_metadata_write", "index" : "metric_metadata" } }
    ]
}'
curl -XPOST $ELASTICSEARCH_URL'/_aliases' -d '
{
    "actions" : [
        { "add" : { "alias" : "metric_metadata_read", "index" : "metric_metadata" } }
    ]
}'
curl -XPOST $ELASTICSEARCH_URL'/_aliases' -d '
{
    "actions" : [
        { "add" : { "alias" : "metric_metadata_read", "index" : "enums" } }
    ]
}'

#add index settings to metric_metadata index
curl -XPOST $ELASTICSEARCH_URL'/metric_metadata/_close'
curl -XPUT $ELASTICSEARCH_URL'/metric_metadata/_settings' -d @index_settings.json
curl -XPOST $ELASTICSEARCH_URL'/metric_metadata/_open'


#add mappings to metric_metadata index
curl -XDELETE $ELASTICSEARCH_URL'/metric_metadata/_mapping/metrics' >& /dev/null
curl -XPUT $ELASTICSEARCH_URL'/metric_metadata/_mapping/metrics' -d @metrics_mapping.json

#add index settings to enums index
curl -XPOST $ELASTICSEARCH_URL'/enums/_close'
curl -XPUT $ELASTICSEARCH_URL'/enums/_settings' -d  @index_settings.json
curl -XPOST $ELASTICSEARCH_URL'/enums/_open'

#add mappings to enums index
curl -XDELETE $ELASTICSEARCH_URL'/enums/_mapping/metrics'  >& /dev/null
curl -XPUT $ELASTICSEARCH_URL'/enums/_mapping/metrics' -d @metrics_mapping_enums.json

#add mappings to graphite_event index
curl -XDELETE $ELASTICSEARCH_URL'/events/_mapping/graphite_event' >& /dev/null
curl -XPUT $ELASTICSEARCH_URL'/events/_mapping/graphite_event' -d @events_mapping.json

