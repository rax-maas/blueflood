#!/bin/bash -x

# This file is customized for vagrant from the heat template 
# https://raw.githubusercontent.com/rackspace-orchestration-templates/grafana/master/grafana-cloud-metrics.yaml
# to setup grafana server along with blue flood finder.

# The following customizations are made
# 1) All variables are changed to bash style variable substitution. Ex: $host_name
# 2) Added logger for graphite-api. Additional configuration for blueflood finder logging.
# 3) Using local code(code present in /vagrant) instead of downloading from github


host_name="grafana"
apache_auth_user="grafana"
apache_auth_password="grafana"
gr_version="1.8.1"
es_version="1.3.4"
rax_tenant="836986"

#staging
#rax_username="<username>"
#rax_apikey="<api key>"
#rax_auth_indicator="rax_auth"
#bf_url=http://staging.metrics.api.rackspacecloud.com

#local
rax_username="local"
rax_apikey="local"
rax_auth_indicator=""
bf_url=http://192.168.50.1:20000

exec 2>&1
exec 1>/tmp/bash-debug.log
ps auxwwef
sleep 120
echo after sleep
ps auxwwef
rm -f /var/lib/dpkg/lock
export DEBIAN_FRONTEND=noninteractive
echo debconf shared/accepted-oracle-license-v1-1 select true | debconf-set-selections
echo debconf shared/accepted-oracle-license-v1-1 seen true | debconf-set-selections
add-apt-repository -y ppa:webupd8team/java
apt-get update -y --force-yes
sleep 5
echo installing packages now, one at a time.
for i in wget oracle-java7-installer vim git nginx nginx-extras apache2-utils python-dev python-setuptools python-pip build-essential libcairo2-dev libffi-dev python-virtualenv python-dateutil ; do
  echo installing "$i"
  apt-get install -y $i --force-yes 2>&1 | tee /tmp/$i.install.log
done
curl -o /tmp/elasticsearch-$es_version.deb https://download.elasticsearch.org/elasticsearch/elasticsearch/elasticsearch-$es_version.deb
dpkg -i /tmp/elasticsearch-$es_version.deb
update-rc.d elasticsearch defaults 95 10
mv /etc/elasticsearch/elasticsearch.yml /etc/elasticsearch/elasticsearch-default.yml
echo cluster.name: es_grafana > /etc/elasticsearch/elasticsearch.yml
echo network.host: 192.168.50.4 >> /etc/elasticsearch/elasticsearch.yml
/etc/init.d/elasticsearch start
htpasswd -b -c /etc/nginx/.htpasswd $apache_auth_user $apache_auth_password
curl -o /tmp/grafana-$gr_version.tar.gz http://grafanarel.s3.amazonaws.com/grafana-$gr_version.tar.gz
tar -xzf /tmp/grafana-$gr_version.tar.gz -C /usr/share/nginx/
ln -s /usr/share/nginx/grafana-$gr_version /usr/share/nginx/grafana
chown -R root:root /usr/share/nginx/grafana-$gr_version
rm /etc/nginx/sites-enabled/default

export heat_one_parameter='$1'
export heat_host_parameter='$host'
cat > /etc/nginx/sites-available/grafana << EOL
upstream graphite {
  server 192.168.50.4:8888;
}
upstream elasticsearch {
  server 192.168.50.4:9200;
}
server {
  listen 80;
  auth_basic 'Restricted';
  auth_basic_user_file /etc/nginx/.htpasswd;
  location /graphite/ {
    rewrite /graphite/(.*) /$heat_one_parameter break;
    proxy_pass http://graphite;
    proxy_redirect off;
    proxy_set_header Host $heat_host_parameter;
  }
  location /elasticsearch/ {
    rewrite /elasticsearch/(.*) /$heat_one_parameter break;
    proxy_pass http://elasticsearch;
    proxy_redirect off;
    proxy_set_header Host $heat_host_parameter;
  }
  location / {
    root /usr/share/nginx/grafana;
  }
}
EOL
ln -s /etc/nginx/sites-available/grafana /etc/nginx/sites-enabled/grafana
/etc/init.d/nginx restart

pip install gunicorn
pip install --upgrade "git+http://github.com/rackerlabs/graphite-api.git@george/fetch_multi_with_patches"

#WE still get the latest code for blueflood_grafana_plugin
git -C /tmp clone https://github.com/rackerlabs/blueflood.git
git -C /tmp/blueflood checkout master
# Using local code instead of downloading from git
# cd /tmp/blueflood/contrib/graphite
cd /vagrant
python setup.py install
cat > /etc/graphite-api.yaml << EOL
search_index: /dev/null
finders:
  - blueflood.TenantBluefloodFinder
functions:
  - graphite_api.functions.SeriesFunctions
  - graphite_api.functions.PieFunctions
time_zone: UTC
blueflood:
  tenant: $rax_tenant
  username: $rax_username
  apikey: $rax_apikey
  authentication_module: $rax_auth_indicator
  authentication_class: BluefloodAuth
  urls:
    - $bf_url
logging:
  version: 1
  disable_existing_loggers: true
  handlers:
    file:
      class: logging.FileHandler
      filename: /var/log/graphite-api.log
  loggers:
    graphite_api:
      handlers:
        - file
      propagate: true
      level: DEBUG
    blueflood_finder:
      handlers:
        - file
      propagate: true
      level: DEBUG
    root:
      handlers:
        - file
      propagate: true
      level: DEBUG    
EOL
cd /usr/share/nginx/grafana/plugins
mkdir datasource
cp -r /tmp/blueflood/contrib/grafana/grafana_1.x/blueflood ./datasource
cat > /usr/share/nginx/grafana/config.js << EOL
define(['settings'],
function (Settings) {
  return new Settings({
    datasources: {
      graphite: {
        type: 'graphite',
        url: 'http://'+window.location.hostname+'/graphite',
      },
      elasticsearch: {
        type: 'elasticsearch',
        url: 'http://'+window.location.hostname+'/elasticsearch',
        index: 'grafana-dash',
        grafanaDB: true,
      },
      RackspaceMetrics: {
        type: 'BluefloodDatasource',
        url: "$bf_url",
        username: "$rax_username",
        apikey: "$rax_apikey",
        tenantID: "$rax_tenant",
        useGraphite: true
      }
    },
    search: {
      max_results: 20
    },
    default_route: '/dashboard/file/default.json',
    unsaved_changes_warning: true,
    playlist_timespan: '1m',
    admin: {
      password: ''
    },
    plugins: {
      panels: [],
      dependencies: ['datasource/blueflood/datasource']
    }
  });
});
EOL
echo $rax_tenant > ~/tenant_id
cat > /etc/init/graphite-api.conf << EOL
description "Graphite-API server"
start on runlevel [2345]
stop on runlevel [!2345]
console log
respawn
exec gunicorn -b 192.168.50.4:8888 -w 8 graphite_api.app:app
EOL
cat > /root/.raxrc << EOL
[credentials]
username=$rax_username
api_key=$rax_apikey
[api]
url=https://monitoring.api.rackspacecloud.com/v1.0
EOL
start graphite-api
wc_notify --data-binary '{"status": "SUCCESS"}'
