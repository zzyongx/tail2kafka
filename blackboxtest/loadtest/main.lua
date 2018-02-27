-- remove the trailing newline
hostshell = "hostname"
pidfile   = "/var/run/tail2kafka.pid"
partition = 0
polllimit = 50
brokers   = "localhost:9092"

rotatedelay = 10
-- optional
pingbackurl = "http://localhost/pingback/tail2kafka"

kafka_global = {
  ["client.id"] = "tail2kafka",
  ["broker.version.fallback"] = "0.8.2.1",
  ["compression.codec"] = "snappy",
  ["max.in.flight"] = 10000,
  ["queue.buffering.max.messages"] = 100000, -- default 100000
  ["queue.buffering.max.kbytes"]   = 512000, -- default 1048576
  ["message.send.max.retries"] = "10",
  ["statistics.interval.ms"] = "60000",
}

kafka_topic  = {
  ["request.required.acks"] = 1,
  ["message.timeout.ms"]    = 0,  -- infinite
}
