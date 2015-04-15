#!/bin/bash

g++ -o tail2kafka tail2kafka.cc ~/soft/librdkafka-0.8.6/src/librdkafka.a -Wall -g -I ~/soft/librdkafka-0.8.6/src -llua-5.1 -lpthread -lrt -lz || exit 1
g++ -o tail2kafka_blackbox tail2kafka_blackbox.cc -g -Wall -I/usr/local/include/librdkafka -lpthread -lrdkafka || exit 1

# delete.topic.enable=true

cd /opt/kafka
for TOPIC in "basic" "filter" "grep" "aggregate" "transform"; do
  bin/kafka-topics.sh --delete --zookeeper localhost:2181 --topic $TOPIC 
	bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic $TOPIC
done
bin/kafka-topics.sh --list --zookeeper localhost:2181
cd -

PIDF=/var/run/tail2kafka.pid
(test -f $PIDF && test -d /proc/$(cat $PIDF)) || ./tail2kafka . &>./tail2kafka.log || exit
./tail2kafka_blackbox
