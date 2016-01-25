#!/bin/bash

### Add repositories

##### Add Java 8 Repo and pre-reqs
sudo apt-get install -y software-properties-common python-software-properties
sudo add-apt-repository -y ppa:webupd8team/java 

###### Set the agreement to the java license so that java7 can be installed without intervention
echo debconf shared/accepted-oracle-license-v1-1 select true | sudo debconf-set-selections
echo debconf shared/accepted-oracle-license-v1-1 seen true | sudo debconf-set-selections

##### Add Cassandra Repo
echo "deb http://debian.datastax.com/community stable main" | sudo tee -a /etc/apt/sources.list.d/cassandra.sources.list
curl -L http://debian.datastax.com/debian/repo_key | sudo apt-key add -

##### Add Elasticsearch Repo
wget -qO - https://packages.elastic.co/GPG-KEY-elasticsearch | sudo apt-key add -
echo "deb http://packages.elastic.co/elasticsearch/1.7/debian stable main" | sudo tee -a /etc/apt/sources.list.d/elasticsearch-1.7.list

##### Update apt
sudo apt-get update -y >/dev/null


### Install apps
## also try '-o=Dpkg::Use-Pty=0' to quiet things down if -q or -qq doesn't work
sudo apt-get -qq install -y oracle-java7-installer cassandra=2.1.1 elasticsearch

##### Cassandra post-install
sudo service cassandra stop
sudo rm -rf /var/lib/cassandra/data/system/*
sudo service cassandra start

##### Elasticsearch post-install
sudo update-rc.d elasticsearch defaults 95 10
sudo service elasticsearch start


### Install Blueflood

##### Get Blueflood stuff

##### Clone repo
git clone https://github.com/rackerlabs/blueflood
cd blueflood
git checkout $GIT_BRANCH_FOR_PACKER # will default to master if empty

##### Grab latest release jar rather than building from scratch
curl -s -L https://github.com/rackerlabs/blueflood/releases/latest | egrep -o 'rackerlabs/blueflood/releases/download/rax-release-.*/blueflood-all-.*-jar-with-dependencies.jar' |  xargs -I % curl -C - -L https://github.com/% --create-dirs -o blueflood-all/target/blueflood-all-2.0.0-SNAPSHOT-jar-with-dependencies.jar

##### Copy upstart config into place
cp contrib/blueflood-packer/upstart/blueflood.conf /etc/init/blueflood.conf

##### chown because, I dunno, why not?
sudo chown -R vagrant:vagrant .

##### Load schema
sleep 10 #TODO: wait for cassandra to be alive a little more gracefully
cqlsh -f ./src/cassandra/cli/load.cdl

##### Start Blueflood
service blueflood start || { echo 'blueflood failed to start' ; exit 1; }

##### iptables rules for all these services
#####
##### note 1: 
#####         these rules get 'duplicated' in the custom Vagrantfile.blueflood.
#####         Remember to add new ports in both places if you are using Vagrant and
#####         want to see things from the point of view of the host machine.
##### note 2: 
#####         Cassandra explicitly cannot bind to 0.0.0.0, as mentioned here:
#####         https://wiki.apache.org/cassandra/FAQ.
#####         While Elasticsearch 1.7 binds to 0.0.0.0, Elasticsearch 2.0 defaults 
#####         to the loopback device.
#####         I figured I would just set these rules up rather than bind netty to 
#####         0.0.0.0 and only have those netty services available from the host.

##### Cassandra to port 19160, in case it's running locally, too
sudo iptables -t nat -I PREROUTING -p tcp -i eth0 --dport 19160 -j DNAT --to-destination 127.0.0.1:9160
##### JMX port for Blueflood
sudo iptables -t nat -I PREROUTING -p tcp -i eth0 --dport 19180 -j DNAT --to-destination 127.0.0.1:9180
##### Elasticsearch to port 19300, in case it's running locally, too
sudo iptables -t nat -I PREROUTING -p tcp -i eth0 --dport 19300 -j DNAT --to-destination 127.0.0.1:9300
##### Ingest port for Blueflood
sudo iptables -t nat -I PREROUTING -p tcp -i eth0 --dport 19000 -j DNAT --to-destination 127.0.0.1:19000
##### Query port for Blueflood
sudo iptables -t nat -I PREROUTING -p tcp -i eth0 --dport 20000 -j DNAT --to-destination 127.0.0.1:20000
##### Save it!
sudo iptables-save