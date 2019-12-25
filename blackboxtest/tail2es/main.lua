-- remove the trailing newline
hostshell = "hostname"
pidfile   = "/var/run/tail2kafka.pid"
polllimit = 50
es_nodes   = {"127.0.0.1:9200"}
es_max_conns = 100

rotatedelay = 10
-- optional
pingbackurl = "http://localhost/pingback/tail2kafka"
