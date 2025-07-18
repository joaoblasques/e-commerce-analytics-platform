
terraform {
  required_providers {
    docker = {
      source  = "kreuzwerker/docker"
      version = "~> 3.0"
    }
  }
}

provider "docker" {
  host = "unix:///var/run/docker.sock"
}

# Docker Network
resource "docker_network" "ecap_network" {
  name = "ecap-network"
}

# Docker Volumes
resource "docker_volume" "postgres_data" {
  name = "ecap-postgres-data"
}

resource "docker_volume" "redis_data" {
  name = "ecap-redis-data"
}

resource "docker_volume" "minio_data" {
  name = "ecap-minio-data"
}

resource "docker_volume" "spark_logs" {
  name = "ecap-spark-logs"
}

resource "docker_volume" "prometheus_data" {
  name = "ecap-prometheus-data"
}

resource "docker_volume" "grafana_data" {
  name = "ecap-grafana-data"
}

resource "docker_volume" "alertmanager_data" {
  name = "ecap-alertmanager-data"
}
