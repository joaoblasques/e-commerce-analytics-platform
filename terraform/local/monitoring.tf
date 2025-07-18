resource "docker_container" "prometheus" {
  name     = "ecap-prometheus"
  image    = "prom/prometheus:v2.47.0"
  hostname = "prometheus"
  networks_advanced {
    name = docker_network.ecap_network.name
  }
  ports {
    internal = 9090
    external = 9090
  }
  volumes {
    host_path = "${path.cwd}/config/prometheus/prometheus.yml"
    container_path = "/etc/prometheus/prometheus.yml"
  }
  volumes {
    host_path = "${path.cwd}/config/prometheus/rules"
    container_path = "/etc/prometheus/rules"
  }
  volumes {
    volume_name = docker_volume.prometheus_data.name
    container_path = "/prometheus"
  }
  command = [
    "--config.file=/etc/prometheus/prometheus.yml",
    "--storage.tsdb.path=/prometheus",
    "--web.console.libraries=/etc/prometheus/console_libraries",
    "--web.console.templates=/etc/prometheus/consoles",
    "--storage.tsdb.retention.time=200h",
    "--web.enable-lifecycle",
    "--web.enable-admin-api"
  ]
  healthcheck {
    test     = ["CMD", "wget", "--no-verbose", "--tries=1", "--spider", "http://localhost:9090/-/healthy"]
    interval = "30s"
    timeout  = "10s"
    retries  = 3
  }
}

resource "docker_container" "grafana" {
  name     = "ecap-grafana"
  image    = "grafana/grafana:10.1.0"
  hostname = "grafana"
  depends_on = [docker_container.prometheus]
  networks_advanced {
    name = docker_network.ecap_network.name
  }
  ports {
    internal = 3000
    external = 3000
  }
  env = [
    "GF_SECURITY_ADMIN_PASSWORD=admin",
    "GF_USERS_ALLOW_SIGN_UP=false",
    "GF_SERVER_ROOT_URL=http://localhost:3000"
  ]
  volumes {
    host_path = "${path.cwd}/config/grafana/provisioning"
    container_path = "/etc/grafana/provisioning"
  }
  volumes {
    volume_name = docker_volume.grafana_data.name
    container_path = "/var/lib/grafana"
  }
  healthcheck {
    test     = ["CMD", "curl", "-f", "http://localhost:3000/api/health"]
    interval = "30s"
    timeout  = "10s"
    retries  = 3
  }
}

resource "docker_container" "alertmanager" {
  name     = "ecap-alertmanager"
  image    = "prom/alertmanager:v0.26.0"
  hostname = "alertmanager"
  depends_on = [docker_container.prometheus]
  networks_advanced {
    name = docker_network.ecap_network.name
  }
  ports {
    internal = 9093
    external = 9093
  }
  volumes {
    host_path = "${path.cwd}/config/alertmanager/alertmanager.yml"
    container_path = "/etc/alertmanager/alertmanager.yml"
  }
  volumes {
    volume_name = docker_volume.alertmanager_data.name
    container_path = "/alertmanager"
  }
  command = [
    "--config.file=/etc/alertmanager/alertmanager.yml",
    "--storage.path=/alertmanager",
    "--web.external-url=http://localhost:9093",
    "--web.route-prefix=/"
  ]
  healthcheck {
    test     = ["CMD", "wget", "--no-verbose", "--tries=1", "--spider", "http://localhost:9093/-/healthy"]
    interval = "30s"
    timeout  = "10s"
    retries  = 3
  }
}

resource "docker_container" "node-exporter" {
  name     = "ecap-node-exporter"
  image    = "prom/node-exporter:v1.6.1"
  hostname = "node-exporter"
  networks_advanced {
    name = docker_network.ecap_network.name
  }
  ports {
    internal = 9100
    external = 9100
  }
  command = [
    "--path.procfs=/host/proc",
    "--path.rootfs=/rootfs",
    "--path.sysfs=/host/sys",
    "--collector.filesystem.mount-points-exclude=^/(sys|proc|dev|host|etc)($$|/)"
  ]
  volumes {
    host_path = "/proc"
    container_path = "/host/proc"
    read_only = true
  }
  volumes {
    host_path = "/sys"
    container_path = "/host/sys"
    read_only = true
  }
  volumes {
    host_path = "/"
    container_path = "/rootfs"
    read_only = true
  }
  healthcheck {
    test     = ["CMD", "wget", "--no-verbose", "--tries=1", "--spider", "http://localhost:9100/metrics"]
    interval = "30s"
    timeout  = "10s"
    retries  = 3
  }
}

resource "docker_container" "postgres-exporter" {
  name     = "ecap-postgres-exporter"
  image    = "prometheuscommunity/postgres-exporter:v0.14.0"
  hostname = "postgres-exporter"
  depends_on = [docker_container.postgres]
  networks_advanced {
    name = docker_network.ecap_network.name
  }
  ports {
    internal = 9187
    external = 9187
  }
  env = [
    "DATA_SOURCE_NAME=postgresql://ecap_user:ecap_password@postgres:5432/ecommerce_analytics?sslmode=disable"
  ]
  healthcheck {
    test     = ["CMD", "wget", "--no-verbose", "--tries=1", "--spider", "http://localhost:9187/metrics"]
    interval = "30s"
    timeout  = "10s"
    retries  = 3
  }
}

resource "docker_container" "redis-exporter" {
  name     = "ecap-redis-exporter"
  image    = "oliver006/redis_exporter:v1.54.0"
  hostname = "redis-exporter"
  depends_on = [docker_container.redis]
  networks_advanced {
    name = docker_network.ecap_network.name
  }
  ports {
    internal = 9121
    external = 9121
  }
  env = [
    "REDIS_ADDR=redis://redis:6379",
    "REDIS_PASSWORD=redis_password"
  ]
  healthcheck {
    test     = ["CMD", "wget", "--no-verbose", "--tries=1", "--spider", "http://localhost:9121/metrics"]
    interval = "30s"
    timeout  = "10s"
    retries  = 3
  }
}

resource "docker_container" "kafka-jmx-exporter" {
  name     = "ecap-kafka-jmx-exporter"
  image    = "danielqsj/kafka-exporter:latest"
  hostname = "kafka-jmx-exporter"
  depends_on = [docker_container.kafka]
  networks_advanced {
    name = docker_network.ecap_network.name
  }
  ports {
    internal = 5556
    external = 5556
  }
  command = ["--kafka.server=kafka:9092"]
  healthcheck {
    test     = ["CMD", "wget", "--no-verbose", "--tries=1", "--spider", "http://localhost:5556/metrics"]
    interval = "30s"
    timeout  = "10s"
    retries  = 3
  }
}