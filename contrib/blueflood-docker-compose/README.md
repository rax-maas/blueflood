## How to run docker-compose

### To run only Blueflood, Cassandra and Elastic-Search 

docker-compose up -d

### To run Blueflood, Cassandra, Elastic-Search and Graphite-API (With BF finder)

docker-compose -f docker-compose-graphite-api.yml up -d