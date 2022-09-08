# Elasticsearch playground

The main goal of this docker environment is to experiment with Elasticsearch, especially regarding upgrades and changing
how indexes are configured. It provides a three-node cluster with relaxed security so that you can jump right in without
worrying about authentication or TLS certificates.

It may also serve as a brief tutorial and introduction to Elasticsearch.
           
## Starting

Before bringing up this environment, you have to ensure this kernel parameter is set to at least 262144 on your host
machine.

```bash
# This only sets the parameter temporarily. You have to modify `/etc/sysctl.conf` to make it permanent.
sudo sysctl -w vm.max_map_count=262144
```
       
After that, the cluster should start right up.

```bash
docker-compose -f contrib/elasticsearch/docker-compose.yml up
```

## Using

Some useful commands to get you started:

### Ingest some data

Write a document to an index every 5 seconds so that you have something to look at.

```bash
while [ true ]; do
  curl "localhost:9200/my-index-write/_doc?pretty" \
    -H "Content-Type: application/json" \
    -d "{\"ts\": $(date +%s)000, \"name\": \"bob\", \"value\": $(($RANDOM % 1000))}"
  sleep 5
done
```

### See the data

As soon as the first document is written, you can see it via a search.         

```bash
curl "localhost:9200/my-index-write/_search?pretty"
```

### See the index

The index was created automatically, and lots of details were filled in for you, like how the data is mapped via a
mapping.

```bash
curl "localhost:9200/my-index-write?pretty"
```

There are lots of stats tracked for an index.

```bash
curl "localhost:9200/my-index-write/_stats?pretty"
```

### Create a different mapping                     

The auto-generated mapping treats the timestamp field as a simple `long` value, not as a date. You can specify your own
mapping to fix that, but you can't alter an existing mapping or change data that's already been indexed under. The best
solution is to start with a fresh index and mapping.

Stop the loop that's writing data to the index, and delete the index.

```bash
curl -X DELETE "localhost:9200/my-index-write?pretty"
```

Create a new index with a different mapping, and then start the loop again.
                                                                            
```bash
curl -X PUT "localhost:9200/my-index-write?pretty" -H "Content-Type: application/json" -d '{
  "mappings": {
    "properties": {
      "ts": {
        "type": "date", 
        "format": "epoch_millis"
      }
    }
  }
}'
```

Note that we only gave information about the `ts` property. The other properties will be mapped automatically when new
data is indexed.

### Use an alias for indirection

An alias is a useful way to present a stable name to clients while changing the index behind the scenes.
       
Stop the loop that's writing data to the index, and delete the index. Then create an index under a different name
aliased to the same name we've been using.

```bash
curl -X PUT "localhost:9200/my-index-1?pretty" -H "Content-Type: application/json" -d '{
  "aliases": {
    "my-index-write": {
      "is_write_index": true
    }
  }
}'
```
    
### Roll over an index

This is a great feature for large indexes that works like log rotation. If you have an index named with a numeric
suffix, like we created in the alias example, you can rotate it automatically. The result will be that the initial index
stays as it is, and a new similar, new index is created with an incremented number at the end of the name. The alias
updates to point to the new index, so clients are unaffected as data starts going into the new index.

Breaking a large index into time-based chunks like this is the correct way to expire and delete old data in
Elasticsearch.

> **NOTE:** I've noticed that if you're only ingesting data and not querying it, Elasticsearch seems not to keep its
document count up to date, so the rollover won't happen as expected. You can sometimes make it update its count and let
the rollover trigger by asking for the count with `curl "localhost:9200/my-index-write/_count?pretty"`

```bash
curl "localhost:9200/my-index-write/_rollover?pretty" -H 'Content-Type: application/json' -d '{
  "conditions": {
    "max_age": "1h",
    "max_docs": 10
  }
}'
```

### Make a read alias

If you break up a large index into chunks, as in the rollover example, you can use a read alias to point to all the
chunks. This will make it appear to clients that there's a single index. It's easy to apply a read alias to all indexes
matching the naming pattern used by rollovers.

```bash
curl -H 'Content-Type: application/json' localhost:9200/_aliases -d '{
  "actions": [{
    "add": {
      "index": "my-index-*",
      "alias": "my-index-read"
    }
  }]
}'
```

If you schedule a periodic task to do a rollover and update the read alias, you've effectively implemented log rotation
for indexed data. You can keep the cluster clean by periodically deleting indexes that are no longer needed.

### Reindex from remote

Copy data from another cluster into this one. You need a `host:port` value of the remote cluster that's reachable from
these containers. You have to put that value in two places:

- in the `source.remote.host` field value in the payload
- in the Elasticsearch `reindex.remote.whitelist` property in the `docker-compose.yml` file

If you change the Elasticsearch property, you need to restart Elasticsearch. Then you can begin the reindex.

> **NOTE**: This only copies the data. You have to set up index settings and mappings prior to reindexing.

```bash
curl "localhost:9200/_reindex" -H "Content-Type: application/json" -d '{
  "source": {
    "remote": {
      "host": "http://172.17.0.1:1234"
    },
    "index": "metric_metadata",
    "query": {
      "match_all": {}
    }
  },
  "dest": {
    "index": "staging_metadata"
  }
}'
```

This begins a reindex `task` that will continue running, even if you interrupt the curl command.

```bash
curl "localhost:9200/_tasks?actions=*reindex\&pretty"
```

One convenient way to make a remote host reachable is with an SSH tunnel.

```bash
ssh -L 172.17.0.1:1234:localhost:9200 <a-host-in-the-remote-cluster>
```
