Filesystem mounts for the minikube containers. These are files that let us pre-configure things like Grafana dashboards
so that they're all set up and ready to use as soon as the k8s yaml is deployed to minikube.

Generally, we get these files by starting up a container, finding its config files or directories, and copying the
default files out of it to here. Then we can tweak the settings to make it work the way we want.

In particular, Grafana is configured to be ready for local use:

- It has the Graphite server set as its default datasource.
- It has some useful Blueflood dashboards already set up and ready to display metrics.
- It allows anonymous admin access so that you can go straight to the dashboards and add/change whatever you want.

Graphite is configured to create up to 1000 new metrics per second instead of the default of 50. Blueflood creates a lot
of metrics.
