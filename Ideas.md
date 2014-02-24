# Project Ideas

These ideas will range from neatly scoped to epic.

__NOTE:__ When adding, please indicate level of complexity on a scale of 0-9 (0==easy, 9==difficult).

#### (7) Integrate with existing dashboard apps like grafana

Blueflood is currently API only. Integrating it with grafana would be really cool. 

#### (7) Local Ingestion Durability

Currently, when data arrives, the HTTP call blocks until data is written to Cassandra. This could be sped up using a local commit log.

#### (7) Use the Cassandra Fat Client

We currently use Astyanax for reading and writing metrics.
This feature would create Reader/Writer implementations that use the Cassandra Fat Client.
This may improve latency of operations as two fewer data marshalling operations would happen per round trip.

#### (9) Internode Cooperation

Shard state is currenly manually configured, and shard owners do not cooperate or communicate with each other at all.
This feature would implement internode communication so that nodes could share enough load state so that shard ownership could be dynamically claimed by nodes.

#### (4) Coda Hale Metrics Integration

Create a reporter module that would allow metrics created by using the Coda Hale Metrics library to be posted to Blueflood

#### (3) Clients

Create a Blueflood client library in $LANGUAGE (java, python, node, ruby, etc.)

#### (5) Explore using a different REST framework.

I would recommend looking at [Dropwizard](https://github.com/dropwizard/dropwizard).
The only sticking points I can think of is that our JSON marshalling may not be conducive to the way Dropwizard does things.
