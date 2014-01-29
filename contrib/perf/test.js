#!/usr/bin/env node

var async = require('async');
var optimist = require('optimist');
var StatsD = require('node-statsd').StatsD,
    client;
//var KeepAliveAgent = require('keep-alive-agent');

var http = require('http');
var util = require('util');

var successes = 0,
    requests = 0,
    keepAliveAgent, argv, reqOpts, reqObj;



var argparsing = optimist
  .usage('\nBenchmark Blueflood ingestion of metrics.\n\nUsage $0 {options}').wrap(150)
  .options('id', {
    'alias': 'tenantId',
    'default': '123456'
  })
  .string('id')
  .options('n', {
    'alias': 'metrics',
    'desc': 'Number of metrics per batch.',
    'default': 1
  })
  .options('i', {
    'alias': 'interval',
    'desc': 'Interval in milliseconds between the reported collected_at time on data points being produced',
    'default': 30000
  })
  .options('d', {
    'alias': 'duration',
    'desc': 'How many minutes ago the first datapoint will be reported as having been collected at.',
    'default': 60
  })
  .options('b', {
    'alias': 'batches',
    'desc': 'Number of batches to send',
    'default': 20
  })
  .options('c', {
    'alias': 'chunked',
    'desc': 'Whether to use chunked encoding',
    'default': false
  })
  .options('r', {
    'alias': 'reports',
    'desc': 'Maximum number of reporting intervals (each 10s), then stop the benchmark',
    'default': 0
  })
  .options('statsd', {
    'desc': 'Whether to report to statsd. Defaults to reporting to a local statsd on default port',
    'default': true
  });


function makeRequest(metrics, callback) {
  var metricsString = JSON.stringify(metrics),
      startTime = new Date().getTime(),
      req = reqObj.request(reqOpts, function(res) {
        if (argv.statsd) {
          client.timing('request_time', new Date().getTime() - startTime);
        }
        if (res.statusCode === 200) {
          successes++;
        } else {
          console.warn(res);
          console.warn('Got status code of ' + res.statusCode);
          res.setEncoding('utf8');
          res.on('data', function (chunk) {
              console.warn('Error Response: ' + chunk);
              process.exit(1);
          });
        }
        res.resume(); // makes it so that we can re-use the connection without having to read the response body
        callback();
      });


  if (!argv.c) {
    req.setHeader('Content-Length', metricsString.length);
  }

  if (metricsString.length > 1048576) {
    console.warn('Exceeding maximum length of 1048576, attempted to send ' + metricsString.length + ' -- Blueflood probably is failing your requests!!');
  }

  req.on('error', function(err) {
    console.error(err);
    process.exit(1)
  });

  req.write(metricsString);
  req.end();
  requests++;
};


// Send argv.n metrics as a batch many times
function sendMetricsForBatch(batchPrefix, callback) {
    // Blueflood understands millis since epoch only
    // Publish metrics with older timestamps (argv.duration minutes before start time)
    var startTime = new Date().getTime(),
        sendTimestamp = startTime - (argv.duration * 1000 * 60),
        j, metrics;
    async.until(
      function done() {
        return sendTimestamp >= startTime;
      },
      function sendOneBatch(callback) {
        metrics = [];
        for (j = 0; j < argv.n; j++) {
          var metric = {};
          metric['collectionTime'] = sendTimestamp;
          metric['metricName'] = batchPrefix + j;
          metric['metricValue'] = Math.random() * 100;
          metric['ttlInSeconds'] = 172800; //(2 * 24 * 60 * 60) //  # 2 days
          metric['unit'] = 'seconds';
          metrics.push(metric);
        }
        sendTimestamp += argv.interval;
        makeRequest(metrics, callback);
      },
      function(err) {
        callback(err);
      });
}


// Send many batches
function sendBatches() {
  var batchPrefixes = [];
  for (var i = 0; i < argv.batches; i++) {
    batchPrefixes.push(i.toString() + '.');
  }
  async.map(batchPrefixes,
            sendMetricsForBatch,
            function(err) {
              reportStatus(true);
              process.exit(err ? 1 : 0);
            });
}


function setupReporting() {
  var startTime = new Date().getTime(),
      lastReqCount = 0,
      firstReqCount, timeTaken, final;
  function reportStatus() {
    timeTaken = new Date().getTime() - startTime;
    if (firstReqCount === undefined) {
      firstReqCount = requests;
    }

    // whether this is final send. helps to automate collecting results of many runs.
    final = (argv.r && (timeTaken >= (argv.r * 10000)));

    console.log(util.format('%d \t %d \t %d \t %d \t %d \t %d \t %dms \t %s',
              (requests * argv.n / (timeTaken / 1000.0)).toFixed(0),
              ((requests - firstReqCount) * argv.n / (timeTaken / 1000.0 - 10)).toFixed(0),
              ((requests - lastReqCount) * argv.n / 10).toFixed(0),
              (requests / (timeTaken / 1000.0)).toFixed(0),
              requests, successes, timeTaken, (final ? "final" : "")));
    lastReqCount = requests;

    if (final) {
      console.log('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~DONE~~~~~~~~~~~~~~~~~~~~~~~~\n\n');
      process.exit(0);
    }
  };

  console.log('Points\tMetrics\tBatches\tM/Batch\tInterv\tDur\tPoints/metric');
  console.log(util.format('%d\t%d\t%d\t%d\t%dms\t%dm\t%d', argv.b * argv.n, argv.n, argv.b, argv.n, argv.i, argv.d, (argv.d * 60000.0 / argv.i).toFixed(0)));
  console.log('M/s\tM/s10+\tM/s-10\tReq/s\tTotal\t2xx\tTime');

  setInterval(reportStatus, 10000);
}


function startup() {
  argv = argparsing.argv;
  if (argv.help) {
    argparsing.showHelp(console.log);
    console.log("M/s -- All time metrics per second.");
    console.log('M/s10+ -- Metrics per second, disregarding the first 10 seconds from starting.');
    console.log('M/s-10 -- Metrics per second during the most recent 10 seconds.');
    console.log('Req/s -- Requests per second');
    console.log('Total -- Total requests made (includes in-progress reqs)');
    console.log('2xx -- Successful requests (only includes completed reqs)');
    console.log('Time -- Total time since starting the script, in milliseconds');
    process.exit(0);
  }

  if (argv.statsd) {
    client = new StatsD();
  }

  reqOpts = {
    host: '127.0.0.1',
    port: 19000,
    path: '/v1.0/' + argv.id + '/experimental/metrics',
    method: 'POST',
    headers: {
        'Content-Type': 'application/json',
        'Connection': 'keep-alive'
    }
  };

  keepAliveAgent = new http.Agent({ keepAlive: true, maxSockets: argv.b });

  // Node version compatibility thing
  if (typeof(keepAliveAgent.request) === 'function') {
    reqObj = keepAliveAgent;
  } else {
    reqOpts.agent = keepAliveAgent;
    reqObj = http;
  }

  setupReporting();
  sendBatches();
}

startup()

