
## Work In Progress

Here's what still needs to be done:

- Instrumentation

Some things that would be totally awesome:

- Java driver based implementation of IMetricsWriter. Eventually this could be accompanied by Java driver-backed
  version of the MetadataCache class.

## Building

JAVA_HOME=WHERE_YOUR_JAVA_7_LIVES mvn clean package -Pcassandra-1.2,skip-unit-tests,skip-integration-tests

This creates an uber jar with everything BUT the com.rackspacecloud.blueflood.dw.* classes. That way, you can do dev
work in your IDE, compile just those files and not have to rebuild the whole jar. It saves time.

## More Loggers

Dropwizard only ships with log appenders for FILE, CONSOLE and SYSLOG logging.

We have added a few custom logging appenders that you are free to use.
You just need to create configuration for them in your config.yaml and enable them from the command line.
Just use these VM arguments:

For airbrake reporting:  `-Dbf.logging.airbrake`

For zeroMQ+logstash reporting: `-Dbf.logging.zmq-logstash`

Sample configuration can be found in this module under `src/main/resources/example-ingest-config.yaml`

## Running

I have included a handy configuration in src/main/resources/example-ingest-config.yaml

    java -agentlib:jdwp=transport=dt_socket,address=34340,suspend=y,server=y
    -cp /PATH/TO/UBER/JAR/blueflood-dropwizard-2.0.0-SNAPSHOT.jar:/PATH/TO/PROJECT/blueflood-dropwizard/target/classes
    com.rackspacecloud.blueflood.dw.ingest.IngestApplication
    server /PATH/TO/PROJECT/blueflood-dropwizard/src/main/resources/example-ingest-config.yaml

## Data

Some fields are optional, but you should specify them all as a matter of habit.

## Sending Data

You deal with the line endings and multiple lines. I've broken them up for clarity.

### Standard Data (single tenant)

    curl -i -H "Content-Type: application/json" -X POST 'http://localhost:8080/v1.0/01234567/ingest/basic' 
    -d '[
      {
        "collectionTime": 1376509892612,
        "ttlInSeconds": 172800,
        "metricValue": 66,
        "metricName": "example.metric.one",
        "tags": ["aaa", "bbb"],
        "metadata": { "ccc": "CCCC", "ddd": "DDDD" }
      }, {
        "collectionTime": 1376509892612,
        "ttlInSeconds": 172800,
        "metricValue": 49.21,
        "metricName": "example.metric.two" 
      }, {
        "collectionTime": 1376509892612,
        "ttlInSeconds": 172800,
        "metricValue": 66,
        "metricName": "example.metric.three"
      } 
    ]'

### Standard Data (scoped tenants)

If your are sending data for multiple tenants, you need to include the tenant ID with each metric.

    curl -i -H "Content-Type: application/json" -X POST 'http://localhost:8080/v1.0/01234567/ingest/basic/scoped'
    -d '[
      { 
        "tenant": "99999", 
        "collectionTime": 1376509892612, 
        "ttlInSeconds": 172800, 
        "metricValue": 66, 
        "metricName": "example.metric.one", 
        "tags": ["aaa", "bbb"], 
        "metadata": { "ccc": "CCCC", "ddd": "DDDD" } 
      }, { 
        "tenant": "888888", 
        "collectionTime": 1376509892612, 
        "ttlInSeconds": 172800, 
        "metricValue": 49.21, 
        "metricName": "example.metric.two" 
      }, { 
        "tenant": "7777777", 
        "collectionTime": 1376509892612, 
        "ttlInSeconds": 172800, 
        "metricValue": 66, 
        "metricName": "example.metric.three" 
      } 
    ]'

### Aggregated data (single tenant)

This is the endpoint that gets used if you are [using statsD to send metrics to Blueflood](https://github.com/gdusbabek/blueflood-statsd-backend).

Histograms, while sent from statsD, are ignored by Blueflood.

    curl -i -H "Content-Type: application/json" -X POST 'http://localhost:8080/v1.0/01234567/ingest/aggregated'
    -d '{
      "collectionTime": 111111111, 
      "gauges": [
        { "name": "gauge_name", "value": 42 }, 
        { "name": "other_gauge", "value": 77 }
      ], 
      "counters": [
        { "name": "counter_name", "value": 32, "rate": 2.32 },
        { "name": "other_counter", "value": 23, "rate": 3.2}
      ], 
      "timers": [
        { 
          "name": "timer_name", 
          "count": 32, 
          "rate": 2.3, 
          "min": 1, 
          "max": 5, 
          "sum": 21, 
          "avg": 2.1, 
          "median": 3, 
          "std": 1.01, 
          "percentiles": { 
            "999": 1.22222, 
            "98": 1.11111
          }, 
          "histogram": { 
            "bin_50": 0, 
            "bin_100": 0, 
            "bin_inf": 0 
          } 
        }, { 
          "name": "other_timer", 
          "count": 332, 
          "rate": 32.3, 
          "min": 31, 
          "max": 35, 
          "sum": 321, 
          "avg": 32.1, 
          "median": 33, 
          "std": 31.01, 
          "percentiles": { 
            "999": 31.22222, 
            "98": 31.11111 
          }, 
          "histogram": { 
            "bin_50": 30, 
            "bin_100": 30, 
            "bin_inf": 30 
          } 
        }
      ], 
      "sets": [
        { 
          "name": "set_name", 
          "values": ["foo", "bar", "baz"] 
        },{
          "name": "other_set", "values":["fiz", "gib", "sid"]
        }
      ] 
    }'

### Aggregated (scoped tenants)

Same as above, but lets you specify tenant per metric.

    curl -i -H "Content-Type: application/json" -X POST 'http://localhost:8080/v1.0/01234567/ingest/aggregated/scoped' 
    -d '{
      "collectionTime": 111111111, 
      "gauges": [
        { "tenant": "12345", "name": "gauge_name", "value": 42 }, 
        { "tenant": "12346", "name": "other_gauge", "value": 77 }
      ], 
      "counters": [
        { "tenant": "12347", "name": "counter_name", "value": 32, "rate": 2.32 },
        { "tenant": "12348", "name": "other_counter", "value": 23, "rate": 3.2}
      ], 
      "timers": [
        { 
          "tenant": "12349", 
          "name": "timer_name", 
          "count": 32, 
          "rate": 2.3, 
          "min": 1, 
          "max": 5, 
          "sum": 21, 
          "avg": 2.1, 
          "median": 3, 
          "std": 1.01, 
          "percentiles": { 
            "999": 1.22222, 
            "98": 1.11111
          }, 
          "histogram": { 
            "bin_50": 0, 
            "bin_100": 0, 
            "bin_inf": 0 
          } 
        }, { 
          "tenant": "12350", 
          "name": "other_timer", 
          "count": 332, 
          "rate": 32.3, 
          "min": 31, 
          "max": 35, 
          "sum": 321, 
          "avg": 32.1, 
          "median": 33, 
          "std": 31.01, 
          "percentiles": { 
            "999": 31.22222, 
            "98": 31.11111 
          }, 
          "histogram": { 
            "bin_50": 30, 
            "bin_100": 30, 
            "bin_inf": 30 
          } 
        }
      ], 
      "sets": [
        {
          "tenant": "12351", 
          "name": "set_name", 
          "values": ["foo", "bar", "baz"] 
        },{
          "tenant": "12352", 
          "name": "other_set", 
          "values":["fiz", "gib", "sid"]
        }
      ] 
    }'
    