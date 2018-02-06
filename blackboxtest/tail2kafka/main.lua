-- remove the trailing newline
hostshell = "hostname"
pidfile   = "/var/run/tail2kafka.pid"
partition = 0
polllimit = 50
brokers   = "127.0.0.1:9092"

rotatedelay = 10
-- optional
pingbackurl = "http://pingbackdst/pingback/tail2kafka"

kafka_global = {
  ["client.id"] = "tail2kafka",
  ["broker.version.fallback"] = "0.8.2.1",
  ["compression.codec"] = "snappy",
  ["max.in.flight"] = 10000,
  ["message.send.max.retries"] = "1",
  ["statistics.interval.ms"] = "60000",
}

kafka_topic  = {
  ["request.required.acks"] = 1,
}
