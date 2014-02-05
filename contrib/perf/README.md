# Performance Benchmarking Toolkit

Tools to benchmark HTTP Ingestion of metric data

## test.js

### Benchmark Blueflood ingestion of metrics.

#### Usage `node ./test.js {options}`

#### Options:

    -n, --metrics     Number of metrics per batch.                                                                                          [default: 1]
    -i, --interval    Interval in milliseconds between the reported collected_at time on data points being produced                     [default: 30000]
    -d, --duration    How many minutes ago the first datapoint will be reported as having been collected at.                               [default: 60]
    -b, --batches     Number of batches to send                                                                                            [default: 20]
    -c, --chunked     Whether to use chunked encoding                                                                                   [default: false]
    -r, --reports     Maximum number of reporting intervals (each 10s), then stop the benchmark                                             [default: 0]
    --statsd          Whether to report to statsd. Defaults to reporting to a local statsd on default port                               [default: true]
    --id, --tenantId                                                                                                                 [default: "123456"]

#### Output Column Explanation:

M/s -- All time metrics per second.

M/s10+ -- Metrics per second, disregarding the first 10 seconds from starting.

M/s-10 -- Metrics per second during the most recent 10 seconds.

Req/s -- Requests per second

Total -- Total requests made (includes in-progress reqs)

2xx -- Successful requests (only includes completed reqs)

Time -- Total time since starting the script, in milliseconds

#### Sample Output:

`> node test.js --metrics 200 --interval 10 --batches 10 --id test --duration 120 -r 1`

    Points	Metrics	Batches	M/Batch	Interv	Dur	Points/metric
    2000	200	10	200ms	10m	120	720000
    M/s	M/s10+	M/s-10	Req/s	Total	2xx	Time
    15388 	 0 	 15400 	 77 	 770 	 760 	 10008ms 	 final
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~DONE~~~~~~~~~~~~~~~~~~~~~~~~

## Automation

There are two shell scripts that can be used to automate benchmarking:
 * resetState.sh
 * automateBenchmarks.sh

### resetState.sh

Used to kill running blueflood and cassandra instances (warning: kills
all running java processes) and remove all cassandra data.

#### Assumptions:

 * Cassandra installed to `/opt/cassandra/`
   * Cass running on port 9160
   * Cass data directory at `/var/cassdata/*`
   * Cass commitlog directory at `/var/casssyscommitlog/*`
 * Blueflood src can be found at `/opt/blueflood/src`
 * No other important java applications are running on the box. They
will be killed.


### automateBenchmarks.sh
 
Automates running benchmarks for a combination of batch count and size.
By default, these will take a very long time. You can modify which
combinations are benchmarked by altering the script. It makes a few
assumptions about what files are named and where they are located.
These should be easy to understand and modify to suit your needs.

