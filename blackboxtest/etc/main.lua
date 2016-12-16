-- remove the trailing newline
hostshell = "echo -n $(hostname)"
pidfile   = "/var/run/tail2kafka.pid"
partition = 0
polllimit = 50
brokers   = "127.0.0.1:9092"

kafka_global = {
  ["client.id"] = "tail2kafka",
  ["broker.version.fallback"] = "0.8.2.1"
}

kafka_topic  = {
  ["request.required.acks"] = 1,
}
