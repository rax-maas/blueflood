FROM java:8

MAINTAINER gaurav.bajaj@rackspace.com

RUN apt-get update
RUN apt-get install -y netcat
RUN apt-get install -y git
RUN apt-get install -y python python-dev python-pip python-virtualenv && \
    rm -rf /var/lib/apt/lists/*
RUN pip install cqlsh==4.1.1

COPY ES-Setup /ES-Setup
COPY load.cdl /blueflood.cdl

COPY artifacts /
RUN ln -s blueflood-all-*-jar-with-dependencies.jar blueflood-all-jar-with-dependencies.jar

ENV MAX_ROLLUP_READ_THREADS=20
ENV MAX_ROLLUP_WRITE_THREADS=5
ENV MAX_TIMEOUT_WHEN_EXHAUSTED=2000
ENV SCHEDULE_POLL_PERIOD=60000
ENV CONFIG_REFRESH_PERIOD=10000
ENV SHARDS=ALL
ENV SHARD_PUSH_PERIOD=2000
ENV SHARD_PULL_PERIOD=20000
ENV SHARD_LOCK_HOLD_PERIOD_MS=1200000
ENV SHARD_LOCK_DISINTERESTED_PERIOD_MS=60000
ENV SHARD_LOCK_SCAVENGE_INTERVAL_MS=120000

ENV GRAPHITE_HOST=
ENV GRAPHITE_PORT=2003

ENV MAX_CASSANDRA_CONNECTIONS=70
ENV CLUSTER_NAME="Test Cluster"
ENV CASSANDRA_MAX_RETRIES=5

ENV ELASTICSEARCH_CLUSTERNAME=blueflood
ENV ELASTICSEARCH_INDEX_NAME_WRITE=metric_metadata_write
ENV ELASTICSEARCH_INDEX_NAME_READ=metric_metadata_read

ENV INGEST_MODE=true
ENV ROLLUP_MODE=true
ENV QUERY_MODE=true

ENV INGESTION_MODULES=com.rackspacecloud.blueflood.service.HttpIngestionService
ENV QUERY_MODULES=com.rackspacecloud.blueflood.service.HttpQueryService
ENV DISCOVERY_MODULES=com.rackspacecloud.blueflood.io.ElasticIO
ENV EVENTS_MODULES=com.rackspacecloud.blueflood.io.EventElasticSearchIO

ENV ZOOKEEPER_CLUSTER=NONE

ENV LOG_LEVEL=INFO
ENV MIN_HEAP_SIZE=1G
ENV MAX_HEAP_SIZE=1G

COPY ./docker-entrypoint.sh /docker-entrypoint.sh
ENTRYPOINT ["/docker-entrypoint.sh"]

EXPOSE 19000
EXPOSE 20000
EXPOSE 9180
