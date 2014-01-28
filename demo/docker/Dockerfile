FROM ubuntu:12.04
# make sure the package repository is up to date
RUN echo "deb http://archive.ubuntu.com/ubuntu precise main universe" > /etc/apt/sources.list
RUN apt-get update

# install net tools
RUN apt-get -y -f install curl net-tools

# install git
RUN apt-get -y install git

# install maven (building blueflood jar)
RUN apt-get -y install maven

# add cassandra PPA
RUN curl -L http://debian.datastax.com/debian/repo_key | apt-key add -
RUN echo "deb http://debian.datastax.com/community/ stable main" >> /etc/apt/sources.list.d/datastax.list

# install python-software-properties (so you can do add-apt-repository)
RUN DEBIAN_FRONTEND=noninteractive apt-get install -y -q python-software-properties

# install oracle java from PPA (cassandra requires oracle java)
RUN add-apt-repository ppa:webupd8team/java -y
RUN apt-get update
RUN echo oracle-java7-installer shared/accepted-oracle-license-v1-1 select true | /usr/bin/debconf-set-selections
RUN apt-get install -y oracle-java7-installer

# Set oracle java as the default java
RUN update-java-alternatives -s java-7-oracle

# install cassandra
RUN apt-get install cassandra -y

# git clone blueflood repo
RUN git clone http://github.com/rackerlabs/blueflood.git /src/blueflood

# start Cassandra
ADD . /src
RUN chmod +x /src/start.sh
EXPOSE 9160
EXPOSE 7199
EXPOSE 19000
EXPOSE 20000
CMD ["/src/start.sh"]
