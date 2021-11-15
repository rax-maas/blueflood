# Blueflood demo

This directory holds some scripts and Docker files to create a temporary Blueflood server and send some metrics to it
that you can then query.

## Build images

First, you need to build the Docker images that will run the server and the client scripts. These aren't published to
any registry. The client image is technically optional, since it only sets up a working Python 3 environment, but I
recommend it so that you don't have to mess with your system Python.

From this demo directory, run:

    docker build -t bf-demo-server server
    docker build -t bf-demo-client client

Note that the server build takes a while because it builds the Blueflood uberjar, which means it has to download all
project dependencies.

## Create a network

To let the demo client and server communicate easily, create a new Docker network for them:

    docker network create blueflood

On the default Docker network, you can only identify containers by IP. In a custom network, container names will resolve
to IPs.

## Start the server

After the image is built, you can start the demo server with

    docker run --rm --network blueflood --name bf-server bf-demo-server

This will start a Blueflood instance that's acting as all roles: ingest, query, and rollup. You'll get all Blueflood
capabilities in this one instance.

When the server has started successfully, you should see a line in the output like

    All blueflood services started

The container is now running in the foreground, and you can send data and requests to it.

## Ingest some data

In another terminal, run

    docker run --rm --network blueflood bf-demo-client ingest.py --host bf-server

If successful, you'll see a few lines of output, including a command to retrieve the metrics just created that starts
with invoking the `retrieve.py` script. You'll also see activity in the server logs as it begins performing rollups of
the new metrics. Use the arguments given for the `retrieve.py` script in the client Docker image to retrieve the
metrics, like

    docker run --rm --network blueflood bf-demo-client retrieve.py [... the provided args ...]
