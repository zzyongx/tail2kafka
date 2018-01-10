#!/bin/bash

CFGDIR="blackboxtest/etc"
PIDF=/var/run/tail2kafka.pid

if [ ! -d $CFGDIR ]; then
  echo "$CFGDIR NOT FOUND"
  echo "disable autoparti"
  echo "main.lua partition=0"
  echo "main.lua pidfile=$PIDF"
  exit 1
fi

# delete.topic.enable=true

find logs -type f -name "*.log" -delete

cd /opt/kafka
for TOPIC in "basic" "filter" "grep" "aggregate" "transform"; do
  bin/kafka-topics.sh --delete --zookeeper localhost:2181 --topic $TOPIC
  bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic $TOPIC
done
bin/kafka-topics.sh --list --zookeeper localhost:2181
cd -

(test -f $PIDF && test -d /proc/$(cat $PIDF)) && kill $(cat $PIDF)
sleep 1
./tail2kafka $CFGDIR || exit $?

sleep 1
./tail2kafka_blackbox
