---
layout: home
title: Home
weight: 0
---

[ ![Build Status] [travis-image] ] [travis]
[ ![Coveralls] [coveralls-image] ] [coveralls]
[ ![Release] [release-image] ] [releases]
[ ![License] [license-image] ] [license]

## Quickstart


There are a couple different ways that you can get started with Blueflood:

### Method 1: Docker
Assuming git and docker installed:

```
git clone -b goru97/docker --single-branch https://github.com/rackerlabs/blueflood.git
cd blueflood/contrib/blueflood-docker/
docker-compose up &
```

* note: this will be in the master branch as soon as [PR 558](https://github.com/rackerlabs/blueflood/pull/558) gets merged

### Method 2: Vagrant
Assuming **[Vagrant] [vagrant-install]** and **[VirtualBox] [virtualbox-install]** installed:

```
mkdir blueflood_demo; cd blueflood_demo
vagrant init blueflood/blueflood; vagrant up
```

## Contributing

Let's just get this straight: we love anybody who submits bug reports, fixes documentation, joins our IRC channel, submits PR's... whatever you got, we'll gladly accept.

If you want to get involved at any level, check out our **[Contributing] [contributing]** page on the wiki!

## Questions or need help?

Check out the **[Talk to us] [talk-to-us]** page on our wiki.


## Copyright and License

Copyright 2013-2015 Rackspace

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

[http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0)

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.



[travis-image]: https://secure.travis-ci.org/rackerlabs/blueflood.png?branch=master
[travis]: http://travis-ci.org/rackerlabs/blueflood
[coveralls-image]: https://coveralls.io/repos/rackerlabs/blueflood/badge.svg?branch=master
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
