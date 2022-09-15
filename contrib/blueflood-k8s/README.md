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
