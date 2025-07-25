# E-Commerce Analytics Platform Helm Chart

This chart deploys the E-Commerce Analytics Platform (ECAP) to a Kubernetes cluster.

## Prerequisites

- Kubernetes 1.19+
- Helm 3.0+

## Installation

1. Add the ECAP Helm repository:

   ```bash
   helm repo add ecap ./helm/ecap
   helm repo update
   ```

2. Install the chart:

   ```bash
   helm install my-ecap ecap/ecap
   ```

## Configuration

The following table lists the configurable parameters of the ECAP chart and their default values.

| Parameter | Description | Default |
|---|---|---|
| `global.environment` | Deployment environment (e.g., `development`, `production`) | `development` |
| `global.imagePullPolicy` | Image pull policy for all containers | `IfNotPresent` |
| `api.replicaCount` | Number of API service replicas | `1` |
| `api.image.repository` | API service image repository | `ecap-api` |
| `api.image.tag` | API service image tag | `latest` |
| `api.service.type` | API service type | `ClusterIP` |
| `api.service.port` | API service port | `8000` |
| `streamingConsumer.replicaCount` | Number of streaming consumer replicas | `1` |
| `streamingConsumer.image.repository` | Streaming consumer image repository | `ecap-streaming-consumer` |
| `streamingConsumer.image.tag` | Streaming consumer image tag | `latest` |
| `streamingConsumer.spark.master` | Spark master URL for streaming consumer | `spark://spark-master:7077` |
| `streamingConsumer.spark.driverMemory` | Spark driver memory for streaming consumer | `1g` |
| `streamingConsumer.spark.executorMemory` | Spark executor memory for streaming consumer | `1g` |
| `streamingConsumer.spark.executorCores` | Spark executor cores for streaming consumer | `1` |
| `transactionProducer.replicaCount` | Number of transaction producer replicas | `1` |
| `transactionProducer.image.repository` | Transaction producer image repository | `ecap-transaction-producer` |
| `transactionProducer.image.tag` | Transaction producer image tag | `latest` |
| `transactionProducer.kafka.bootstrapServers` | Kafka bootstrap servers for transaction producer | `kafka:9092` |
| `transactionProducer.kafka.topic` | Kafka topic for transaction producer | `transactions` |
| `userBehaviorProducer.replicaCount` | Number of user behavior producer replicas | `1` |
| `userBehaviorProducer.image.repository` | User behavior producer image repository | `ecap-user-behavior-producer` |
| `userBehaviorProducer.image.tag` | User behavior producer image tag | `latest` |
| `userBehaviorProducer.kafka.bootstrapServers` | Kafka bootstrap servers for user behavior producer | `kafka:9092` |
| `userBehaviorProducer.kafka.topic` | Kafka topic for user behavior producer | `user-events` |
| `dashboard.replicaCount` | Number of dashboard replicas | `1` |
| `dashboard.image.repository` | Dashboard image repository | `ecap-dashboard` |
| `dashboard.image.tag` | Dashboard image tag | `latest` |
| `dashboard.service.type` | Dashboard service type | `ClusterIP` |
| `dashboard.service.port` | Dashboard service port | `8501` |
| `sparkMaster.replicaCount` | Number of Spark master replicas | `1` |
| `sparkMaster.image.repository` | Spark master image repository | `bitnami/spark` |
| `sparkMaster.image.tag` | Spark master image tag | `3.4.1` |
| `sparkMaster.service.type` | Spark master service type | `ClusterIP` |
| `sparkMaster.service.ports.web.port` | Spark master web UI port | `8080` |
| `sparkMaster.service.ports.spark.port` | Spark master RPC port | `7077` |
| `sparkWorker.replicaCount` | Number of Spark worker replicas | `1` |
| `sparkWorker.image.repository` | Spark worker image repository | `bitnami/spark` |
| `sparkWorker.image.tag` | Spark worker image tag | `3.4.1` |
| `sparkWorker.spark.masterUrl` | Spark master URL for workers | `spark://spark-master:7077` |
| `sparkWorker.spark.workerMemory` | Spark worker memory | `2G` |
| `sparkWorker.spark.workerCores` | Spark worker cores | `2` |
| `postgresql.image.repository` | PostgreSQL image repository | `postgres` |
| `postgresql.image.tag` | PostgreSQL image tag | `15-alpine` |
| `postgresql.environment.POSTGRES_DB` | PostgreSQL database name | `ecommerce_analytics` |
| `postgresql.environment.POSTGRES_USER` | PostgreSQL user | `analytics_user` |
| `postgresql.environment.POSTGRES_PASSWORD` | PostgreSQL password | `dev_password` |
| `postgresql.service.type` | PostgreSQL service type | `ClusterIP` |
| `postgresql.service.port` | PostgreSQL service port | `5432` |
| `postgresql.persistence.enabled` | Enable PostgreSQL persistence | `true` |
| `postgresql.persistence.size` | PostgreSQL persistence volume size | `1Gi` |
| `redis.image.repository` | Redis image repository | `redis` |
| `redis.image.tag` | Redis image tag | `7-alpine` |
| `redis.service.type` | Redis service type | `ClusterIP` |
| `redis.service.port` | Redis service port | `6379` |
| `redis.persistence.enabled` | Enable Redis persistence | `true` |
| `redis.persistence.size` | Redis persistence volume size | `1Gi` |
| `minio.image.repository` | MinIO image repository | `minio/minio` |
| `minio.image.tag` | MinIO image tag | `latest` |
| `minio.environment.MINIO_ROOT_USER` | MinIO root user | `minioadmin` |
| `minio.environment.MINIO_ROOT_PASSWORD` | MinIO root password | `minioadmin` |
| `minio.service.type` | MinIO service type | `ClusterIP` |
| `minio.service.port` | MinIO API port | `9000` |
| `minio.service.consolePort` | MinIO console port | `9001` |
| `minio.persistence.enabled` | Enable MinIO persistence | `true` |
| `minio.persistence.size` | MinIO persistence volume size | `10Gi` |
| `kafka.image.repository` | Kafka image repository | `confluentinc/cp-kafka` |
| `kafka.image.tag` | Kafka image tag | `7.4.0` |
| `kafka.kafka.brokerId` | Kafka broker ID | `1` |
| `kafka.kafka.advertisedListeners` | Kafka advertised listeners | `PLAINTEXT://kafka:9092` |
| `kafka.kafka.offsetsTopicReplicationFactor` | Kafka offsets topic replication factor | `1` |
| `kafka.kafka.groupInitialRebalanceDelayMs` | Kafka group initial rebalance delay | `0` |
| `kafka.kafka.jmxPort` | Kafka JMX port | `9997` |
| `kafka.service.type` | Kafka service type | `ClusterIP` |
| `kafka.service.port` | Kafka service port | `9092` |
| `kafka.persistence.enabled` | Enable Kafka persistence | `true` |
| `kafka.persistence.size` | Kafka persistence volume size | `10Gi` |
| `zookeeper.image.repository` | Zookeeper image repository | `confluentinc/cp-zookeeper` |
| `zookeeper.image.tag` | Zookeeper image tag | `7.4.0` |
| `zookeeper.clientPort` | Zookeeper client port | `2181` |
| `zookeeper.tickTime` | Zookeeper tick time | `2000` |
| `zookeeper.persistence.enabled` | Enable Zookeeper persistence | `true` |
| `zookeeper.persistence.dataSize` | Zookeeper data volume size | `1Gi` |
| `zookeeper.persistence.logSize` | Zookeeper log volume size | `1Gi` |

Specify each parameter using the `--set key=value[,key=value]` argument to `helm install`.

Alternatively, a YAML file that specifies the values for the parameters can be provided via the `-f` flag.

## Uninstalling the Chart

To uninstall/delete the `my-ecap` deployment:

```bash
helm uninstall my-ecap
```

The command removes all the Kubernetes components associated with the chart and deletes the release.
