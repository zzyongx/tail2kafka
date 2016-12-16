hostshell = "echo -n $(hostname)"
pidfile   = "/var/run/tail2kafka.pid"
brokers   = "127.0.0.1:9092"
pollLimit = 1000

kafka_global = {
  ["client.id"] = "tail2kafka",
  ["broker.version.fallback"] = "0.8.2.1"
}

kafka_topic  = {
  ["request.required.acks"] = 1,
}
