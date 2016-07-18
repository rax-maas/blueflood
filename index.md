---
layout: home
title: Home
weight: 0
---

[ ![Build Status] [travis-image] ] [travis] [ ![Coveralls] [coveralls-image] ] [coveralls] [ ![Release] [release-image] ] [releases] [ ![License] [license-image] ] [license]

## What is Blueflood?

Simply put, Blueflood is a big, fast database for your metrics.  

Data from Blueflood can be used to construct dashboards, generate reports, graphs or for any other use involving time-series data.  It focuses on near-realtime data, with data that is queryable mere milliseconds after ingestion.

Data is stored using Cassandra to make Blueflood fault-tolerant and highly-available.  In contrast to forebearers such as [CarbonDB](http://carbondb.org/) or [RRDTool](http://oss.oetiker.ch/rrdtool/), your Blueflood cluster can expand as your metrics needs grow.  Simply add more Cassandra nodes.  

Written in Java, Blueflood exists as a cluster of distributed services.  The services are:

* Ingestion
* Query 
* Rollup

You send metrics to the ingestion service.  You query your metrics from the Query service.  And in the background, rollups are batch-processed offline so that queries for large time-periods are returned quickly.

Blueflood was created by the [Rackspace Monitoring](http://www.rackspace.com/cloud/monitoring) team at [Rackspace](http://www.rackspace.com/) to manage raw metrics generated from the Rackspace Monitoring system.  Blueflood is now largely promoted by the [Rackspace Metrics](https://www.rackspace.com/knowledge_center/article/rackspace-metrics-overview) team which provides Blueflood-as-a-Service as a free service (yes, really -- just [talk to us] [talk-to-us]).  Since making the product open source, several other large companies have begun using the tool.

Blueflood is an open source under the [Apache 2.0 license] [license].


## Quickstart


There are a couple different ways that you can get started with Blueflood:

### Method 1: Docker
Assuming git and docker installed:

```
git clone https://github.com/rackerlabs/blueflood.git
cd blueflood/contrib/blueflood-docker-compose/
docker-compose up -d
```

### Method 2: Vagrant
Assuming **[Vagrant] [vagrant-install]** and **[VirtualBox] [virtualbox-install]** installed:

```
mkdir blueflood_demo; cd blueflood_demo
vagrant init blueflood/blueflood; vagrant up
```

## Media

* **Metrics Meetup**, December 2014 - *Blueflood: Multi-tenanted Time-series Datastore*:
	* [Video](https://vimeo.com/114585521)
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

Let's just get this straight: we love anybody who submits bug reports, fixes documentation, joins our IRC channel, submits PR's... whatever you got, we'll gladly accept.

If you want to get involved at any level, check out our **[Contributing] [contributing]** page on the wiki!

## Questions or need help?

Check out the **[Talk to us] [talk-to-us]** page on our wiki.


## Copyright and License

Copyright 2013-2015 Rackspace

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

[http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0)

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.


[travis-image]: https://img.shields.io/travis/rackerlabs/blueflood/master.svg
[travis]: http://travis-ci.org/rackerlabs/blueflood
[coveralls-image]: https://img.shields.io/coveralls/rackerlabs/blueflood/master.svg
[coveralls]: https://coveralls.io/github/rackerlabs/blueflood
[release-image]: http://img.shields.io/badge/rax-release-v1.0.1956.svg
[releases]: https://github.com/rackerlabs/blueflood/releases
[license-image]: https://img.shields.io/badge/license-Apache%202-blue.svg
[license]: http://www.apache.org/licenses/LICENSE-2.0

[wiki]: https://github.com/rackerlabs/blueflood/wiki
[talk-to-us]: https://github.com/rackerlabs/blueflood/wiki/Talk-to-us
[contributing]: https://github.com/rackerlabs/blueflood/wiki/Contributing


[vagrant-install]: http://docs.vagrantup.com/v2/installation/index.html
[virtualbox-install]: https://www.virtualbox.org/wiki/Downloads
