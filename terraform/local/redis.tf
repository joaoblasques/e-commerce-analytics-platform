
resource "docker_container" "redis" {
  name     = "ecap-redis"
  image    = "redis:7-alpine"
  hostname = "redis"
  networks_advanced {
    name = docker_network.ecap_network.name
  }
  ports {
    internal = 6379
    external = 6379
  }
  command = ["redis-server", "--appendonly", "yes", "--requirepass", "redis_password"]
  volumes {
    volume_name = docker_volume.redis_data.name
    container_path = "/data"
  }
  healthcheck {
    test     = ["CMD", "redis-cli", "-a", "redis_password", "ping"]
    interval = "30s"
    timeout  = "10s"
    retries  = 3
  }
}
