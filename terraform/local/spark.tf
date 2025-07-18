resource "docker_container" "spark-master" {
  name     = "ecap-spark-master"
  image    = "bitnami/spark:3.4.1"
  hostname = "spark-master"
  networks_advanced {
    name = docker_network.ecap_network.name
  }
  ports {
    internal = 8080
    external = 8080
  }
  ports {
    internal = 7077
    external = 7077
  }
  env = [
    "SPARK_MODE=master",
    "SPARK_RPC_AUTHENTICATION_ENABLED=no",
    "SPARK_RPC_ENCRYPTION_ENABLED=no",
    "SPARK_LOCAL_STORAGE_ENCRYPTION_ENABLED=no",
    "SPARK_SSL_ENABLED=no",
    "SPARK_MASTER_OPTS=-Dspark.deploy.defaultCores=2 -Dspark.eventLog.enabled=true -Dspark.eventLog.dir=file:///opt/spark-events"
  ]
  volumes {
    volume_name = docker_volume.spark_logs.name
    container_path = "/opt/bitnami/spark/logs"
  }
  volumes {
    volume_name = docker_volume.spark_logs.name
    container_path = "/opt/spark-events"
  }
  healthcheck {
    test     = ["CMD", "curl", "-f", "http://localhost:8080"]
    interval = "30s"
    timeout  = "10s"
    retries  = 3
  }
}

resource "docker_container" "spark-worker-1" {
  name     = "ecap-spark-worker-1"
  image    = "bitnami/spark:3.4.1"
  hostname = "spark-worker-1"
  depends_on = [docker_container.spark-master]
  networks_advanced {
    name = docker_network.ecap_network.name
  }
  ports {
    internal = 8081
    external = 8081
  }
  env = [
    "SPARK_MODE=worker",
    "SPARK_MASTER_URL=spark://spark-master:7077",
    "SPARK_WORKER_MEMORY=2g",
    "SPARK_WORKER_CORES=2",
    "SPARK_RPC_AUTHENTICATION_ENABLED=no",
    "SPARK_RPC_ENCRYPTION_ENABLED=no",
    "SPARK_LOCAL_STORAGE_ENCRYPTION_ENABLED=no",
    "SPARK_SSL_ENABLED=no"
  ]
  volumes {
    volume_name = docker_volume.spark_logs.name
    container_path = "/opt/bitnami/spark/logs"
  }
  volumes {
    volume_name = docker_volume.spark_logs.name
    container_path = "/opt/spark-events"
  }
  healthcheck {
    test     = ["CMD", "curl", "-f", "http://localhost:8081"]
    interval = "30s"
    timeout  = "10s"
    retries  = 3
  }
}

resource "docker_container" "spark-worker-2" {
  name     = "ecap-spark-worker-2"
  image    = "bitnami/spark:3.4.1"
  hostname = "spark-worker-2"
  depends_on = [docker_container.spark-master]
  networks_advanced {
    name = docker_network.ecap_network.name
  }
  ports {
    internal = 8081
    external = 8082
  }
  env = [
    "SPARK_MODE=worker",
    "SPARK_MASTER_URL=spark://spark-master:7077",
    "SPARK_WORKER_MEMORY=2g",
    "SPARK_WORKER_CORES=2",
    "SPARK_RPC_AUTHENTICATION_ENABLED=no",
    "SPARK_RPC_ENCRYPTION_ENABLED=no",
    "SPARK_LOCAL_STORAGE_ENCRYPTION_ENABLED=no",
    "SPARK_SSL_ENABLED=no"
  ]
  volumes {
    volume_name = docker_volume.spark_logs.name
    container_path = "/opt/bitnami/spark/logs"
  }
  volumes {
    volume_name = docker_volume.spark_logs.name
    container_path = "/opt/spark-events"
  }
  healthcheck {
    test     = ["CMD", "curl", "-f", "http://localhost:8081"]
    interval = "30s"
    timeout  = "10s"
    retries  = 3
  }
}

resource "docker_container" "spark-history" {
  name     = "ecap-spark-history"
  image    = "rangareddy1988/spark-history-server:latest"
  hostname = "spark-history"
  depends_on = [docker_container.spark-master]
  networks_advanced {
    name = docker_network.ecap_network.name
  }
  ports {
    internal = 18080
    external = 18080
  }
  env = [
    "SPARK_HISTORY_UI_PORT=18080",
    "SPARK_HISTORY_RETAINEDAPPLICATIONS=200",
    "SPARK_HISTORY_UI_MAXAPPLICATIONS=500"
  ]
  volumes {
    volume_name = docker_volume.spark_logs.name
    container_path = "/tmp/spark-events"
  }
  healthcheck {
    test     = ["CMD", "curl", "-f", "http://localhost:18080"]
    interval = "30s"
    timeout  = "10s"
    retries  = 3
  }
}