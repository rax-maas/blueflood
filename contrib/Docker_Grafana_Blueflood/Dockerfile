FROM python:2.7

MAINTAINER gaurav.bajaj@rackspace.com

RUN apt-get update -y --force-yes && \
    apt-get -y --no-install-recommends install libfontconfig curl ca-certificates && \
    apt-get clean && \
    curl https://grafanarel.s3.amazonaws.com/builds/grafana_2.6.0_amd64.deb > /tmp/grafana.deb && \
    dpkg -i /tmp/grafana.deb && \
    rm /tmp/grafana.deb && \
    curl -L https://github.com/tianon/gosu/releases/download/1.7/gosu-amd64 > /usr/sbin/gosu && \
    chmod +x /usr/sbin/gosu && \
    apt-get remove -y curl && \
    apt-get autoremove -y && \
    rm -rf /var/lib/apt/lists/*

RUN apt-get update && \
    apt-get install -y git --force-yes && \
    apt-get install -y build-essential --force-yes && \
    apt-get install -y libcairo2-dev --force-yes && \
    apt-get install -y libffi-dev --force-yes && \
    pip install gunicorn &&\
	pip install --upgrade "git+http://github.com/rackerlabs/graphite-api.git@1.1.3-rax.1" &&\
	git -C /tmp clone https://github.com/rackerlabs/blueflood.git &&\
    git -C /tmp/blueflood checkout master &&\
	cd /tmp/blueflood/contrib/graphite &&\
	python setup.py install 

VOLUME ["/var/lib/grafana", "/var/lib/grafana/plugins", "/var/log/grafana", "/etc/grafana"]

EXPOSE 3000
EXPOSE 8888

COPY ./docker-entrypoint.sh /docker-entrypoint.sh

ENTRYPOINT ["/docker-entrypoint.sh"]