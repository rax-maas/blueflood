FROM ubuntu:20.04
ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get update
RUN apt-get install -y python3 python3-pip
RUN pip install requests
ADD . /src
WORKDIR /src
ENTRYPOINT ["python3"]
