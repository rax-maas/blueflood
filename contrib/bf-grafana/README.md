This Docker composition runs an end-to-end instance of Blueflood along with Grafana 3. The composition is started using:

```bash
docker-compose up -d
```

Grafana can be accessed at http://localhost:3000 with the default login of:
* **User**: admin
* **Password**: admin

The first thing you'll need to do after logging in is create a new data source with the choices:
* **Name**: Blueflood
* **Type**: Blueflood
* **Url**: http://bf_finder:8888
* **Access**: proxy

After you have saved that data source you can create graphs in a new dashboard that reference "Blueflood" as the
"Panel data source".

Unless you already have something that feeds into Blueflood, you can manually ingest some metrics by `POST`ing like this:

```bash
curl -X POST \
  http://localhost:19000/v2.0/100/ingest \
  -H 'Content-Type: application/json' \
  -d '[
      {
        "collectionTime": 1515694269000,
        "ttlInSeconds": 172800,
        "metricValue": 66,
        "metricName": "example.metric.one"
      },
      {
        "collectionTime": 1515694279000,
        "ttlInSeconds": 172800,
        "metricValue": 69,
        "metricName": "example.metric.one"
      }
    ]'
```

Be sure to adjust the `collectionTime` to something near the current time in milliseconds since Blueflood will reject
ingesting of old metrics by default.