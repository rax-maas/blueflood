## How to run this container?

This image comes with a set of default environment variables, which runs the Blueflood container decently. If you want to run it in production environment or with some other settings, you can always adjust to taste.

Here's the list of ENV variables and their description.

| Variable | Description | default |
| ----- | ------- | --------- |
| CASSANDRA_HOST | IP address of Cassandra seed. (Required) | null |
| ELASTICSEARCH_HOST | IP address of Elasticsearch node. (Required) | null |
| MAX_ROLLUP_READ_THREADS | Maximum number of read threads participating in rolling up the metrics | 20 |
| MAX_ROLLUP_WRITE_THREADS | Maximum number of write threads participating in rolling up the metrics | 5 |
| MAX_CASSANDRA_CONNECTIONS | Maximum number of connections with each Cassandra node | 70 |
| INGEST_MODE | Whether to start the Ingest service | true |
| ROLLUP_MODE | Whether to start the Rollup service | true |
| QUERY_MODE | Whether to start the Query service | true |
| LOG_LEVEL | BF services Logging Level. See here for detailed description: https://logging.apache.org/log4j/1.2/apidocs/org/apache/log4j/Level.html | INFO |
| MIN_HEAP_SIZE | Initial size of the heap to be allocated to BF process. | 1G |
| MAX_HEAP_SIZE | Maximum size of the heap to be allocated to BF process. | 1G |
| GRAPHITE_HOST | IP address of the Graphite host to monitor your container | " " |
| GRAPHITE_PORT | Line port of the Graphite host to monitor your container | 2003 |
| GRAPHITE_PREFIX | Prefix for graphite metrics. | Host name of the container. |

For a complete list of variables to use, see: https://github.com/rackerlabs/blueflood/blob/master/blueflood-core/src/main/java/com/rackspacecloud/blueflood/service/CoreConfig.java

### To run only Blueflood, Cassandra and Elastic-Search 

```
docker-compose up -d
```
### To run Blueflood, Cassandra, Elastic-Search and Graphite-API (With BF finder)

```
docker-compose -f docker-compose-graphite-api.yml up -d
```