hostshell = "echo -n $(sogou-host | grep -v rsync)"
pidfile   = "/var/run/tail2kafka.pid"
brokers   = "127.0.0.1:9092"

kafka_global = {
  ["client.id"] = "tail2kafka",
}

kafka_topic  = {
  ["request.required.acks"] = 1,
}
