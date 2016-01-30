<p align="center">
 <img src="http://blueflood.io/images/bf-bg-color.png" width="220" height="232" align=center>
</p>

# Blueflood 
[ ![Build Status] [travis-image] ] [travis]
[ ![Coveralls] [coveralls-image] ] [coveralls]
[ ![Release] [release-image] ] [releases]
[ ![License] [license-image] ] [license]

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

The best place to start is the [10 minute guide](https://github.com/rackerlabs/blueflood/wiki/10-Minute-Guide).
In a nutshell, you must do this:

    java -cp /path/to/uber.jar \
    -Dblueflood.config=file:///path/to/blueflood.conf \
    -Dlog4j.configuration=file:///path/to/log4j.properties \
    com.rackspacecloud.blueflood.service.BluefloodServiceStarter
    
Each configuration option can be found in Configuration.java.  Each of those can be overridden on the command line by
doing:

    -DCONFIG_OPTION=NEW_VALUE

## Contributing

First, we welcome bug reports and contributions.
If you would like to contribute code, just fork this project and send us a pull request.
If you would like to contribute documentation, you should get familiar with
[our wiki](https://github.com/rackerlabs/blueflood/wiki)

Also, we have set up a [Google Group](https://groups.google.com/forum/#!forum/blueflood-discuss) to answer questions.

If you prefer IRC, most of the Blueflood developers are in #blueflood on Freenode. 

## License

Copyright 2013-2015 Rackspace

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0 

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

[travis-image]: https://img.shields.io/travis/rackerlabs/blueflood/master.svg
[travis]: http://travis-ci.org/rackerlabs/blueflood
[coveralls-image]: https://img.shields.io/coveralls/rackerlabs/blueflood/master.svg
[coveralls]: https://coveralls.io/github/rackerlabs/blueflood
[release-image]: http://img.shields.io/badge/rax-release-v1.0.1956.svg
[releases]: https://github.com/rackerlabs/blueflood/releases
[license-image]: https://img.shields.io/badge/license-Apache%202-blue.svg
[license]: http://www.apache.org/licenses/LICENSE-2.0
