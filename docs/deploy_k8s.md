# Deploying Transfer

The Transfer is a sophisticated data ingestion engine that enables you to handle large amounts of data movement.
We strongly believe in cloud-native technologies, and see **transfer** as a driven power for open-source data-platforms build on top of clouds.
To quickly deploy **transfer** on your local machine you can visit the [Quickstart](./getting_started.md) guide.

## Understanding the Transfer Deployment

Transfer is built to be deployed into a Kubernetes cluster.

You can use a Cloud Provider, such as, AWS, GCP, Azure, or onto a single node, such as an EC2 VM, or even locally on your computer.

We highly recommend deploying **transfer** using Helm and the documented Helm chart values.

Helm is a Kubernetes package manager for automating deployment and management of complex applications with microservices on Kubernetes.
Refer to our [Helm Chart Usage Guide](../helm) for more information about how to get started.

## Installation Guide

This installation guide walks through how to deploy **transfer** into _any_ kubernetes cluster. It will run through how to deploy a default version of Transfer. It will, as an optional step, describe you how you can customize that deployment for your cloud provider and integrations (e.g. ingresses, external databases, external loggers, etc).

This guide assumes that you already have a running kubernetes cluster. If you're trying out **transfer** on your local machine, we recommend using [Docker Desktop](https://www.docker.com/products/docker-desktop/) and enabling the kubernetes extension. We've also tested it with kind, k3s, and colima. If you are installing onto a single vm in a cloud provider (e.g. EC2 or GCE), make sure you've installed kubernetes on that machine. This guide also works for multi-node setups (e.g. EKS and GKE).

### 1. Add the Helm Repository

The deployment will use a Helm chart which is a package for Kubernetes applications, acting like a blueprint or template that defines the resources needed to deploy an application on a Kubernetes cluster. Charts are stored in `helm-repo`.

To add a remote helm repo:

1. Run: `helm pull oci://ghcr.io/transferia/transferia-helm/transfer`. It will pull latest **transfer** helm chart locally

2. After adding the repo, perform the repo indexing process by running `helm repo update`.

### 2. Create a Namespace for Transfer

While it is not strictly necessary to isolate the **transfer** installation into its own namespace, it is good practice and recommended as a part of the installation.
This documentation assumes that you chose the name `transfer` for the namespace, but you may choose a different name if required.

To create a namespace run the following:

```sh
kubectl create namespace transfers
```

### 3. Create a values.yaml override file

To configure your installation of transfer, you will need to override specific parts of the Helm Chart. To do this you should create a new file called `values.yaml` somewhere that is accessible during the installation process.
The documentation has been created to "build up" a values.yaml, so there is no need to copy the whole of the Chart values.yaml. You only need to provide the specific overrides.

```yaml
image: ghcr.io/transferia/transferia:v0.0.0-rc8
env:
  AWS_REGION: eu-central-1

coordinator:
  type: s3 # type of coordination, one of: s3 or memory
  bucket: place_your_bucket # Bucket with write access, will be used to store state
transferSpec:
  id: mytransfer # Unique ID of a transfer
  name: awesome transfer # Human friendly name for a transfer
  type: INCREMENT_ONLY # type of transfer, one of: INCREMENT_ONLY, SNAPSHOT_ONLY, SNAPSHOT_AND_INCREMENT
  src:
    type: source_type # for example: pg, s3, kafka ...
    params: ... # source type params, all params can be founded in `model_source.go` for provider folder
  dst:
    type: target_type # for example: s3, ch, kafka ...
    params: ... # target type params, all params can be founded in `model_destination.go` for provider folder
```

### 4. Installing Transfer

After you have applied your Secret values to the Cluster and you have filled out a values.yaml file appropriately for your specific configuration, you can begin a Helm Install. To do this, make sure that you have the [Helm Client](https://helm.sh/docs/intro/install/) installed and on your path.
Then you can run:

```sh
helm upgrade NAME_OF_TRANSFER \
  --namespace NAME_OF_NAMESPACE oci://ghcr.io/transferia/transferia-helm/transfer \
  --values PATH_TO_VALUES_FILE \
  --install
```

After the installation has completed. Once it's completed you will see **NAME_OF_TRANSFER** in your deployements, as statefull set (for INCREMENTAL transfers) or as CRON_JOB (for SNAPSHOT_ONLY).

```shell
> kubectl get pods --namespace NAME_OF_NAMESPACE

NAME                                      READY   STATUS    RESTARTS   AGE
transfer-NAME_OF_TRANSFER-replication-0   1/1     Running   0          23m
```

### 5. (optional for customized installations only) Setup Monitoring

By default, transfer pods will expose metrics on `9091` ports, and add a default prometheus annotations:

```yaml
annotations:
  prometheus.io/path: /metrics
  prometheus.io/port: "9091"
  prometheus.io/scrape: "true"
```

This will expose metrics scrapper for k8s scrapper. You can re-use our grafana dashboard template from Grafana template [here](../assets/grafana.tmpl.json).

This template will generate for you something like this:

![demo_grafana_dashboard.png](_assets/demo_grafana_dashboard.png)

Be caution: in template you must replace **<Your-Prometheus-source-ID>** before deployement to grafana.

#### Add a prefix to metrics

To add a prefix to metrics, use the `--metrics-prefix` flag. For example, to add a prefix of `transfer` to all metrics, use the following command:

```
trcli replicate --metrics-prefix transfer ...
```

Note: `--metrics-prefix` flag is only available for `activate`, `replicate` and `upload` commands.

### 6. Secrets management

For secrets management we recommend to use env-vars in paar with secret operator, for example [Hashicorp Vault](https://developer.hashicorp.com/vault/docs/platform/k8s/injector/examples)

#### specify secrets in transferSpec

Use ENV-vars substitute for src and dst params, you may specify

##### Hashicorp Vault

[Hashicorp Vault](https://developer.hashicorp.com/vault/docs/platform/k8s/injector/examples)

```yaml
transferSpec:
  dst:
    type: ch
    params:
      Password: "${FOO}"

env:
  FOO: "/vault/secret"
```

##### K8s secret

[k8s secret](https://kubernetes.io/docs/concepts/configuration/secret/)

```yaml
secret_env:
  - env_name: FOO
    secret_name: k8s_secret_name
    secret_key: k8s_secret_key
```

##### ENV-vars

Don't recommend to use this way, but you may specify env-vars directly in values.yaml

```yaml
env:
  FOO: "secret"
```

#### After load

After load this transfer yaml would be:

```yaml
dst:
  type: ch
  params:
    Password: "secret"
```
