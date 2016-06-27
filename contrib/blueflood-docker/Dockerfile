FROM java:7

MAINTAINER gaurav.bajaj@rackspace.com

RUN apt-get update
RUN apt-get install -y netcat
RUN apt-get install -y git
RUN apt-get install -y python python-dev python-pip python-virtualenv && \
    rm -rf /var/lib/apt/lists/*
RUN pip install cqlsh==4.1.1

RUN git clone https://github.com/rackerlabs/blueflood.git
RUN mv ./blueflood/blueflood-elasticsearch/src/main/resources ./ES-Setup
RUN mv ./blueflood/src/cassandra/cli/load.cdl ./blueflood.cdl
RUN rm -r ./blueflood

RUN curl -s -L https://github.com/rackerlabs/blueflood/releases/latest | egrep -o 'rackerlabs/blueflood/releases/download/rax-release-.*/blueflood-all-.*-jar-with-dependencies.jar' |  xargs -I % curl -C - -L https://github.com/% --create-dirs -o ./blueflood-all-2.0.0-SNAPSHOT-jar-with-dependencies.jar

ENV MAX_ROLLUP_THREADS=20
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
ENV ENUMS_DISCOVERY_MODULES=com.rackspacecloud.blueflood.io.EnumElasticIO

ENV ZOOKEEPER_CLUSTER=NONE

ENV LOG_LEVEL=INFO
ENV MIN_HEAP_SIZE=1G
ENV MAX_HEAP_SIZE=1G

COPY ./docker-entrypoint.sh /docker-entrypoint.sh
ENTRYPOINT ["/docker-entrypoint.sh"]

EXPOSE 19000
EXPOSE 20000
EXPOSE 9180
