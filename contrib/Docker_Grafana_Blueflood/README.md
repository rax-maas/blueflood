# Description:
Docker Container to run Grafana - 2.6.0 on Blueflood / Rackspace Metrics.

# Building / Pulling:
docker build -t goru97/grafana-blueflood . </BR>
or </BR>
docker pull goru97/grafana-blueflood

# Running:

### With local Blueflood Setup:

docker run -p 3000:3000 -e BLUEFLOOD_QUERY_URL=<YOUR_BLUEFLOOD_URL> -e TENANT_ID=<YOUR_TENANT_ID> goru97/grafana-blueflood

### With Rackspace Metrics:

docker run -p 3000:3000 -e BLUEFLOOD_QUERY_URL=https://global.metrics.api.rackspacecloud.com -e TENANT_ID=$YOUR_TENANT_ID -e RAX_USERNAME=$YOUR_RACKSPACE_USERNAME -e RAX_APIKEY=$YOUR_RACKSPACE_APIKEY goru97/grafana-blueflood

## Setting Up Grafana:

* Sign - Up
* Add Organization
* Add API-Key for role-based access / To use HTTP-APIs
* Add Datasource (Option 1 - Using GUI):
 
  * Add Blueflood as name of the datasource
  * Select Graphite as the Type
  * Set the URL to http://localhost:8888

* Add Datasource (Option 2 - Using CURL):

```
curl -H "Content-Type: application/json" -H "Authorization: Bearer <API-KEY-FROM-GUI>" -X POST -d '{ "name":"Blueflood", "type":"graphite", "url":"http://localhost:8888", "access":"proxy", "basicAuth":false }' http://grafana-host:3000/api/datasources

```

