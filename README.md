<p align="center">
 <img src="http://blueflood.io/images/bf-bg-color.png" width="220" height="232" align=center>
</p>

# Blueflood

[![Unit tests](https://github.com/rax-maas/blueflood/actions/workflows/unit-test.yml/badge.svg?branch=master)](https://github.com/rax-maas/blueflood/actions/workflows/unit-test.yml)
[![Coverage Status](https://coveralls.io/repos/github/rax-maas/blueflood/badge.svg?branch=master)](https://coveralls.io/github/rax-maas/blueflood?branch=master)
[![License](https://img.shields.io/badge/license-Apache%202-blue.svg)](http://www.apache.org/licenses/LICENSE-2.0)

[Discuss](https://groups.google.com/forum/#!forum/blueflood-discuss) - [Code](http://github.com/rackerlabs/blueflood) - [Site](http://blueflood.io)

## Introduction

Blueflood is a multi-tenant, distributed metric processing system. Blueflood is capable of ingesting, rolling up and serving metrics at a massive scale.  

## Getting Started

The latest code will always be here on Github.

    git clone https://github.com/rax-maas/blueflood.git
    cd blueflood

## Building

Blueflood builds and runs on Java 8. Ensure you're using an appropriate JDK before proceeding.

Blueflood builds with Maven. Use typical Maven lifecycle phases:

- `mvn clean` removes build artifacts.
- `mvn test` runs unit tests.
- `mvn verify` runs all tests.
- `mvn package` builds a Blueflood package for release.

Important build profiles to know about:

- `skip-unit-tests` skips unit tests in all modules.
- `skip-integration-tests` skips the integration tests.

Blueflood's main artifact is an ['uber jar'](http://stackoverflow.com/questions/11947037/what-is-an-uber-jar), produced
by the [`blueflood-all` module](blueflood-all/pom.xml).

After compiling, you can also build a Docker image with `mvn docker:build`. See
[blueflood-docker](contrib/blueflood-docker) for the Docker-related files.

### Running

You can easily build a ready-to-run Blueflood jar from source:

    mvn package -P skip-unit-tests,skip-integration-tests

However, it requires Cassandra to start and Elasticsearch for all its features to work. The best place to start is the
[10 minute guide](https://github.com/rackerlabs/blueflood/wiki/10-Minute-Guide).

## Additional Tools

The Blueflood team maintains a number of tools that are related to the project, but not essential components of it.
These tools are kept in various other repos:

* Performance Tests: Scripts for load testing a blueflood installation using [The
  Grinder](http://grinder.sourceforge.net/). https://github.com/rackerlabs/raxmetrics-perf-test-scripts

* Carbon Forwarder: a process that receives data from carbon (one of the components of
  [Graphite](https://graphiteapp.org/)) and sends it to a Blueflood instance.
  https://github.com/rackerlabs/blueflood-carbon-forwarder

* Blueflood-Finder: a plugin for graphite-web and graphite-api that allows them to using a Blueflood instance as a data
  backend. https://github.com/rackerlabs/blueflood-graphite-finder

* StatsD plugin: a statsD backend that sends metrics a Blueflood instance.
  https://github.com/rackerlabs/blueflood-statsd-backend

## Contributing

First, we welcome bug reports and contributions.
If you would like to contribute code, just fork this project and send us a pull request.
If you would like to contribute documentation, you should get familiar with
[our wiki](https://github.com/rackerlabs/blueflood/wiki)

Also, we have set up a [Google Group](https://groups.google.com/forum/#!forum/blueflood-discuss) to answer questions.

## License

Copyright 2013-2017 Rackspace

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0 

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
