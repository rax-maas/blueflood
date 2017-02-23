FROM ubuntu:16.04

RUN apt-get update

RUN apt-get -y -f install curl net-tools git maven

# add cassandra PPA
RUN apt-key adv --fetch-keys https://debian.datastax.com/debian/repo_key && \
    echo 'deb     "https://debian.datastax.com/community/" stable main' > /etc/apt/sources.list.d/datastax.list

# install oracle java from PPA (cassandra requires oracle java)
RUN apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 7B2C3B0889BF5709A105D03AC2518248EEA14886 && \
    echo 'deb     "http://ppa.launchpad.net/webupd8team/java/ubuntu" xenial main' > /etc/apt/sources.list.d/webupd8team.list && \
    echo 'deb-src "http://ppa.launchpad.net/webupd8team/java/ubuntu" xenial main' > /etc/apt/sources.list.d/webupd8team.list && \
    apt-get update

RUN echo 'oracle-java7-installer shared/accepted-oracle-license-v1-1 select true' | /usr/bin/debconf-set-selections
RUN apt-get install -y oracle-java7-installer

# Set oracle java as the default java
RUN update-java-alternatives -s java-7-oracle

# install cassandra
RUN apt-get install -y cassandra

# git clone Blueflood repo
RUN git clone http://github.com/rackerlabs/blueflood.git /opt/blueflood

# start Cassandra
ADD . /opt
RUN chmod +x /opt/start.sh

EXPOSE 9160
EXPOSE 7199
EXPOSE 19000
EXPOSE 20000

CMD ["/opt/start.sh"]
