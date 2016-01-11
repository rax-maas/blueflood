# blueflood_grafana_plugin_1.9.x

Download latest 1.x.x release and unpack into <your grafana installation>/plugins/datasource/.
Then edit Grafana config.js

###Add dependencies</br>

plugins: { </br>
  panels: [],</br>
  dependencies: ['datasource/blueflood/datasource']</br>
}</br>

###Add datasource and setup your Blueflood url</br>

datasources: {</br>
  ...</br>
  blueflood: {</br>
    type: 'BluefloodDatasource',</br>
    url: 'http://staging.metrics.api.rackspacecloud.com',</br>
    username: [UserName],</br>
    apikey: [APIKey],</br>
    tenantID: [tenantID]</br>
  }</br>
  },</br>
