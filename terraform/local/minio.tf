
resource "docker_container" "minio" {
  name     = "ecap-minio"
  image    = "minio/minio:RELEASE.2024-01-16T16-07-38Z"
  hostname = "minio"
  networks_advanced {
    name = docker_network.ecap_network.name
  }
  ports {
    internal = 9000
    external = 9000
  }
  ports {
    internal = 9001
    external = 9001
  }
  env = [
    "MINIO_ROOT_USER=minioadmin",
    "MINIO_ROOT_PASSWORD=minioadmin123"
  ]
  command = ["server", "/data", "--console-address", ":9001"]
  volumes {
    volume_name = docker_volume.minio_data.name
    container_path = "/data"
  }
  healthcheck {
    test     = ["CMD", "curl", "-f", "http://localhost:9000/minio/health/live"]
    interval = "30s"
    timeout  = "10s"
    retries  = 3
  }
}
