FROM java:7

MAINTAINER gaurav.bajaj@rackspace.com

RUN apt-get update
RUN apt-get install -y netcat
RUN apt-get install -y python python-dev python-pip python-virtualenv && \
    rm -rf /var/lib/apt/lists/*
RUN pip install cqlsh

ADD ./configs/ .

RUN curl -s -L https://github.com/rackerlabs/blueflood/releases/latest | egrep -o 'rackerlabs/blueflood/releases/download/rax-release-.*/blueflood-all-.*-jar-with-dependencies.jar' |  xargs -I % curl -C - -L https://github.com/% --create-dirs -o ./blueflood-all-2.0.0-SNAPSHOT-jar-with-dependencies.jar

CMD ["/startBlueflood.sh"]

EXPOSE 19000
EXPOSE 20000
EXPOSE 9180