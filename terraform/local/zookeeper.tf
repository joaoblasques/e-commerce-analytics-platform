
resource "docker_container" "zookeeper" {
  name     = "ecap-zookeeper"
  image    = "confluentinc/cp-zookeeper:7.4.0"
  hostname = "zookeeper"
  networks_advanced {
    name = docker_network.ecap_network.name
  }
  ports {
    internal = 2181
    external = 2181
  }
  env = [
    "ZOOKEEPER_CLIENT_PORT=2181",
    "ZOOKEEPER_TICK_TIME=2000"
  ]
  healthcheck {
    test     = ["CMD", "nc", "-z", "localhost", "2181"]
    interval = "30s"
    timeout  = "10s"
    retries  = 3
  }
}
