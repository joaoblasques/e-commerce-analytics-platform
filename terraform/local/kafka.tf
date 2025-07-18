
resource "docker_container" "kafka" {
  name     = "ecap-kafka"
  image    = "confluentinc/cp-kafka:7.4.0"
  hostname = "kafka"
  depends_on = [docker_container.zookeeper]
  networks_advanced {
    name = docker_network.ecap_network.name
  }
  ports {
    internal = 9092
    external = 9092
  }
  ports {
    internal = 9101
    external = 9101
  }
  env = [
    "KAFKA_BROKER_ID=1",
    "KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181",
    "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT",
    "KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092",
    "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1",
    "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1",
    "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1",
    "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0",
    "KAFKA_JMX_PORT=9101",
    "KAFKA_JMX_HOSTNAME=localhost",
    "KAFKA_AUTO_CREATE_TOPICS_ENABLE=true"
  ]
  volumes {
    volume_name = docker_volume.kafka_data.name
    container_path = "/var/lib/kafka/data"
  }
  healthcheck {
    test     = ["CMD", "kafka-topics", "--bootstrap-server", "localhost:9092", "--list"]
    interval = "30s"
    timeout  = "10s"
    retries  = 3
  }
}

resource "docker_volume" "kafka_data" {
  name = "ecap-kafka-data"
}
