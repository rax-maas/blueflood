<p align="center">
 <img src="http://i.imgur.com/dJwsM5z.gif" width="500" height="350" align=center>
</p>

# Blueflood [![Build Status](https://secure.travis-ci.org/rackerlabs/blueflood.png)](http://travis-ci.org/rackerlabs/blueflood) [![Coverage Status](https://coveralls.io/repos/rackerlabs/blueflood/badge.svg)](https://coveralls.io/r/rackerlabs/blueflood)

[Discuss](https://groups.google.com/forum/#!forum/blueflood-discuss) - [Code](http://github.com/rackerlabs/blueflood) - [Site](http://blueflood.io)

## Introduction

Blueflood is a multi-tenant, distributed metric processing system. Blueflood is capable of ingesting, rolling up and serving metrics at a massive scale.  

## Getting Started

The latest code will always be here on Github.

    git clone https://github.com/rackerlabs/blueflood.git
    cd blueflood
    
You can run the entire suite of tests using Maven:

    mvn test integration-test

### Building

Build an ['uber jar'](http://stackoverflow.com/questions/11947037/what-is-an-uber-jar) using maven:

    mvn package -P all-modules

The uber jar will be found in ${BLUEFLOOD_DIR}/blueflood-all/target/blueflood-all-${VERSION}-jar-with-dependencies.jar.
This jar contains all the dependencies necessary to run Blueflood with a very simple classpath.

### Running

The best place to start is the [10 minute guide](https://github.com/rackerlabs/blueflood/wiki/10minuteguide).
In a nutshell, you must do this:

    java -cp /path/to/uber.jar \
    -Dblueflood.config=file:///path/to/blueflood.conf \
    -Dlog4j.configuration=file:///path/to/log4j.properties \
    com.rackspacecloud.blueflood.service.BluefloodServiceStarter
    
Each configuration option can be found in Configuration.java.  Each of those can be overridden on the command line by
doing:

    -DCONFIG_OPTION=NEW_VALUE

## Development

We anticipate different use cases for Blueflood.  For example, at Rackspace it made more sense to create a
[Thrift](http://thrift.apache.org) layer for ingestion and query.  We have chosen not to release that layer because
it contains a lot of code that is specific to our infrastructure and other backend systems.

We decided to release Blueflood with reference HTTP-based ingestion and query layers.  These layers may be replaced by
code that works better with your enterprise.

### Custom Ingestion

Several things must be done to properly ingest data:
1. Full resolution data must be written via `AstyanaxWriter.insertFull()`.
2. A `ScheduleContext` object must be `update()`d regarding that metrics shard and collection time.
3. Shard state must be periodically pushed to the database for each shard that metrics have been collected for.  This
   can be done by getting the dirty slot information from the `ShardStateManager` associated with a particular
   `ScheduleContext` object.

`HttpMetricsIngestionServer` is an example of how to set up a multi-threaded staged ingestion pipeline.

### Custom Querying

Thankfully, querying is easier than ingestion.  Whatever query service you create should have a handler that extends
`RollupHandler`, which provides a basic wrapping of low level read operations provided by `AstyanaxReader`.

## Operations

Blueflood exposes a great deal of internal performance metrics over
[JMX](https://blogs.oracle.com/jmxetc/entry/what_is_jmx).
Blueflood respects the standard JMX JVM settings:

    com.sun.management.jmxremote.authenticate
    com.sun.management.jmxremote.ssl
    java.rmi.server.hostname
    com.sun.management.jmxremote.port
    
You can use any tool that supports JMX to get internal performance metrics out of Blueflood.

Additionally, internal performance metrics can be pushed directly to a [Graphite](http://graphite.wikidot.com/) 
service by specifying the following in your Blueflood
configuration:

    GRAPHITE_HOST
    GRAPHITE_PORT
    GRAPHITE_PREFIX

## Media

* **Metrics Meetup**, December 2014 - *Blueflood: Multi-tenanted Time-series Datastore*:
	* [Video]() - TBD
	* [Slides](https://raw.githubusercontent.com/rackerlabs/blueflood/master/contrib/presentations/MetricsMeetupDecember2014.pdf)
* **Berlin Buzzwords**, May 2014 - *Blueflood 2.0 and beyond: Distributed open source metrics processing*:
  * [Video](https://www.youtube.com/watch?v=NmZTdWzX5v8&list=PLq-odUc2x7i-Q5gQtkmba4ov37XRPjp6n&index=33)
  * [Slides](http://berlinbuzzwords.de/sites/berlinbuzzwords.de/files/media/documents/gary_dusbabek_berlin_buzzwords_2014.pdf)
* **Metrics Meetup**, February 2014 - *Introduction to Blueflood*:
	* [Video](http://vimeo.com/87210602)
	* [Slides](http://www.lakshmikannan.me/slides/2014-02-19-sf-metrics-meetup/#/)
	* This presentation given to the [SF Metrics Meetup](http://www.meetup.com/San-Francisco-Metrics-Meetup/) group in February 2014 is a good video introduction to Blueflood.
* **Cassandra EU**, October 2013 - *Blueflood: Simple Metrics Processing*:
	* [Video](https://www.youtube.com/watch?v=1rcffSq26z0)
	* [Slides](http://www.slideshare.net/gdusbabek/blueflood-open-source-metrics-processing-at-cassandraeu-2013)


## Contributing

First, we welcome bug reports and contributions.
If you would like to contribute code, just fork this project and send us a pull request.
If you would like to contribute documentation, you should get familiar with
[our wiki](https://github.com/rackerlabs/blueflood/wiki)

Also, we have set up a [Google Group](https://groups.google.com/forum/#!forum/blueflood-discuss) to answer questions.

If you prefer IRC, most of the Blueflood developers are in #blueflood on Freenode. 

If you prefer hipchat, here is the link: https://www.hipchat.com/gQPx7fG8u

## License

Copyright 2013 Rackspace

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
