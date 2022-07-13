# Blueflood in minikube

This is a basic, working Blueflood in minikube, complete with Cassandra, Elasticsearch, and Grafana dashboards. If all
goes as planned, then as long as you've already [installed minikube](https://minikube.sigs.k8s.io/docs/start/), it's
just a few steps to get it running.

## Getting it started

> **NOTE**: The commands here assume your working directory is the root of the Blueflood project. If you're in the
`contrib/blueflood-minikube` directory, make sure to adjust the paths accordingly.

First, start minikube:

```bash
minikube start
```
                                               
Next, you have to mount the blueflood project directory into minikube with:

```bash
minikube mount $(pwd):/blueflood
```

Then, if you want to run the latest Blueflood image from DockerHub, you can go ahead and start the stack with `kubectl
apply -f contrib/blueflood-minikube/blueflood.yaml`. If you prefer to run a locally built version of Blueflood, you can
build a Docker image inside minikube. You just need to set a couple of variables to do that. First, run `minikube
docker-env`, and note the values of `DOCKER_HOST` and `DOCKER_CERT_PATH` for use in the following commands. Replace
`tcp://` in `DOCKER_HOST` with `https://` when you use it below. Then you can build blueflood and run it with

```bash
mvn package docker:build -DdockerHost=$MODIFIED_DOCKER_HOST -DdockerCertPath=$DOCKER_CERT_PATH
kubectl apply -f contrib/blueflood-minikubeblueflood.yaml
```

> **NOTE**: You can use `-P skip-unit-tests,skip-integration-tests` to skip all the tests and make packaging faster!          

This should give you a complete, running Blueflood stack within a minute or two. The longest startup is Cassandra,
possibly because it's looking for other nodes. Configuration might help with that.

## Talking to it

Minikube runs Kubernetes in a virtual environment in its own private network. Here are some ways you can communicate
with it.

### Using minikube IPs

Most simply, you can get an IP and port reachable from your host machine with

```bash
minikube service blueflood --url
```

This returns three items. The first is the Java debug address. The second is the ingest URL. The third is the query URL.

### Using port forwarding

If you prefer to bind it to ports on one of your host's interfaces, you can forward Blueflood's ingest and query ports
with

```bash
kubectl port-forward blueflood 19000
kubectl port-forward blueflood 20000
```

If you want to reach Blueflood from a different host interface, like from another running Docker container, just tell it
which interface to use. If your Docker interface has IP `172.17.0.1`, then:

```bash
kubectl port-forward blueflood --address 172.17.0.1 19000
kubectl port-forward blueflood --address 172.17.0.1 20000
```

## Seeing it work

Use any of these methods, and plug the appropriate URLs into the example scripts in the [10 minute
guide](https://github.com/rackerlabs/blueflood/wiki/10-Minute-Guide) to see it working.
