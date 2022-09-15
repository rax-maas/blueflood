FROM eclipse-temurin:8

MAINTAINER gaurav.bajaj@rackspace.com
MAINTAINER ryan.stewart@rackspace.com

RUN apt-get update && apt-get install -y \
    netcat \
    python3 \
    python3-dev \
    python3-pip \
    python3-virtualenv && \
    rm -rf /var/lib/apt/lists/*

RUN pip install cqlsh==6.0.0

COPY ES-Setup /ES-Setup
COPY load.cdl /blueflood.cdl

COPY artifacts /
RUN ln -s blueflood-all-*-jar-with-dependencies.jar blueflood-all-jar-with-dependencies.jar

COPY ./docker-entrypoint.sh /docker-entrypoint.sh
ENTRYPOINT ["/docker-entrypoint.sh"]

EXPOSE 19000
EXPOSE 20000
EXPOSE 9180
