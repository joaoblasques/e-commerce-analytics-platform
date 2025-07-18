
resource "docker_container" "postgres" {
  name     = "ecap-postgres"
  image    = "postgres:15-alpine"
  hostname = "postgres"
  networks_advanced {
    name = docker_network.ecap_network.name
  }
  ports {
    internal = 5432
    external = 5432
  }
  env = [
    "POSTGRES_DB=ecommerce_analytics",
    "POSTGRES_USER=ecap_user",
    "POSTGRES_PASSWORD=ecap_password",
    "POSTGRES_INITDB_ARGS=--auth-host=scram-sha-256"
  ]
  volumes {
    volume_name = docker_volume.postgres_data.name
    container_path = "/var/lib/postgresql/data"
  }
  volumes {
    host_path = "${path.cwd}/config/postgres/init"
    container_path = "/docker-entrypoint-initdb.d"
  }
  healthcheck {
    test     = ["CMD-SHELL", "pg_isready -U ecap_user -d ecommerce_analytics"]
    interval = "30s"
    timeout  = "10s"
    retries  = 3
  }
}
