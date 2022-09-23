# Blueflood for Kubernetes

Building on lessons learned from [blueflood-minikube](../blueflood-minikube), this makes a fully deployable Blueflood
Kubernetes descriptor.

Start by getting your [kubectl](https://kubernetes.io/docs/tasks/tools/) connected to the cluster you want to deploy to.

This project uses [Kustomize](https://kubernetes.io/docs/tasks/manage-kubernetes-objects/kustomization/) as a light
layer of management to reduce duplication in the normal K8s resources and manage ConfigMaps. The k8s resource files may
be used the way they are, but they can be customized via overlays, courtesy of Kustomize.

## General organization

Following Kustomize's recommended layout, `base` contains the main set of resources. For organizational purposes,
resources are grouped into files according to their service, such as `cassandra.yaml` or `elasticsearch.yaml`.

All resources in a given yaml file have a label named `component` whose value is equal to the file base name. Therefore,
all Cassandra resources are labeled with `component=cassandra`. This makes managing resources in the k8s cluster much
simpler. This bears repeating: *all* resources here are assigned to a component. If a resource doesn't have a
`component` label, it shouldn't exist.

The general setup of a yaml file is as follows.

- There's a group of pods to run the actual service, organized as either a [Deployment](
  https://kubernetes.io/docs/concepts/workloads/controllers/deployment/) or a
  [StatefulSet](https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/). Both ensure a certain number of
  pods stay running with your desired configuration. A StatefulSet also ensures that if a pod dies, its replacement will
  be assigned the same persistent volume that the old one was using, which is very important for data stores.

- There's a [Service](https://kubernetes.io/docs/concepts/services-networking/service/) with a
  [ClusterIP](https://kubernetes.io/docs/concepts/services-networking/service/#publishing-services-service-types) that
  makes the set of pods available to other things in the cluster via DNS.

- Pods expect a [ConfigMap](https://kubernetes.io/docs/concepts/configuration/configmap/) to be present in the
  cluster with all necessary config files. By convention, the ConfigMap should be named `<component>-config`. As an
  example, the ConfigMap for Elasticsearch is named `elasticsearch-config`. The files from the ConfigMap are mounted as a
  directory in the pods. A good way to find what config files go in the ConfigMap is to start an instance of the pod's
  image and copy the default config files out of it. ConfigMaps are easy to manage with [Kustomize's configMapGenerator](
  https://kubernetes.io/docs/tasks/manage-kubernetes-objects/kustomization/#configmapgenerator).

- Kubernetes supports mounting a ConfigMap directly to a file system as either a file or a directory of files. Often,
  though, the ConfigMap isn't mounted directly onto the real pod due to ownership or access issues. Instead, pods have a
  small [Volume]( https://kubernetes.io/docs/concepts/storage/volumes/) dedicated to config files. An
  [InitContainer](https://kubernetes.io/docs/concepts/workloads/pods/init-containers/) prepares the volume by copying the
  files to it from the ConfigMap and setting appropriate ownership, file mode, etc.
 
- If necessary, pods use a [PersistentVolumeClaim](https://kubernetes.io/docs/concepts/storage/persistent-volumes/) to
  request persistent storage that will endure a pod restart. This is for pods that need non-ephemeral data and probably
  means it's in a StatefulSet.

- For groups of pods that form a cluster (Cassandra and Elasticsearch), there's a headless Service. This doesn't provide
  a ClusterIP. Instead, it resolves in DNS to the IPs of all the cluster members, making it easier to do cluster
  discovery.

> **NOTE:** Each yaml file is fully self-contained except for the required ConfigMap. After creating an appropriate
ConfigMap, you should be able to `kubectl apply` any single yaml file and get a complete set of running resources! Any
changes needed for deployment in different environments should be made via a Kustomize overlay.

## First time startup

First, create a namespace for everything to live in. The name of the namespace should match that set by the
kustomization.yaml in your selected overlay. For example, using the "example" overlay provided here:

```bash
kubectl create ns blueflood-demo
```

You can't bring all pods up at the same time. There are two stages to a first-time startup. You have to do some initial
setup for Cassandra and Elasticsearch first.

First, if using Elasticsearch 7+, uncomment the `cluster.initial_master_nodes` property from the elasticsearch.yml
you're using. It's required for first time cluster start. It's unclear if anything like this applies to versions <7.

Next, the Kubernetes health probes hinder initial cluster formation for both Elasticsearch and Cassandra. You have to
comment out all the probes for the Cassandra seed nodes and the Elasticsearch nodes. Look for "DON'T PROBE HERE!" in
`cassandra.yaml` and `elasticsearch.yaml`.

Then start Cassandra and Elasticsearch with

```bash
kubectl apply -k contrib/blueflood-k8s/overlays/example/ -l 'component in (cassandra, elasticsearch)'
```

It'll take some time for the respective clusters to stabilize and become responsive. Expect some pod restarts as this
happens. Cassandra is especially sensitive to starting multiple nodes at the same time. Some nodes will fail because
others are initializing. Within a handful of restarts, both clusters should become responsive.

### Elasticsearch

The Elasticsearch cluster will likely be the first to fully start up. Check its health by looking for `number_of_nodes`
== 3 (or how many replicas you've set it to) and `status` == "green" in the output of

```bash
kubectl exec es-master-0 -c elasticsearch -- curl 'localhost:9200/_cluster/health?pretty'
```

Once the cluster is up and running, initialize the Elasticsearch indexes and mappings by running an appropriate init
script from the [elasticsearch module's resources](../../blueflood-elasticsearch/src/main/resources), like

```bash
kubectl port-forward service/elasticsearch 9200
blueflood-elasticsearch/src/main/resources/init-es-6/init-es.sh
```

Finally, revert the config changes you made:

- Uncomment the health probes that you commented out previously in `elasticsearch.yaml`.

- For Elasticsearch 7+, comment out the `cluster.initial_master_nodes` property in the `elasticsearch.yml`.

Then do a `kubectl apply` to update the cluster. Kubernetes will do a rolling restart of the pods for you.

### Cassandra

Then check the Cassandra cluster health. There should be four nodes (or however many replicas you've configured) with a
status of `UN` (Up Normal).

```bash
kubectl exec cass-seed-0 -c cassandra -- nodetool status
```

Once the cluster is up and running, set up the Cassandra schema by running the [cqlsh
script](../../src/cassandra/cli/load.cdl) on one of the nodes, such as

```bash
kubectl cp src/cassandra/cli/load.cdl cass-seed-0:/bf-init.cdl -c cassandra
kubectl exec cass-seed-0 -c cassandra -- cqlsh -f /bf-init.cdl
```

Finally, bring back the health probes that you commented out previously by uncommenting them and doing a `kubectl apply`
to update the cluster. Kubernetes will do a rolling restart of the seed nodes for you.
