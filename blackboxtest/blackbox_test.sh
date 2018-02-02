#!/bin/bash

CFGDIR="blackboxtest/etc"
PIDF=/var/run/tail2kafka.pid
BINDIR=$(readlink -e $(dirname $BIN))
BUILDDIR=$BINDIR/../build

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

OLDFILE=kafka2filedir/basic/zzyong_basic.log.old
test -d kafka2filedir || mkdir kafka2filedir
rm -f kafka2filedir/basic.0.offset $OLDFILE

$BUILDDIR/kafka2file 127.0.0.1:9092 basic 0 kafka2filedir &
KAFKA2FILE_ID=$!

(test -f $PIDF && test -d /proc/$(cat $PIDF)) && kill $(cat $PIDF)
sleep 1
$BUILDDIR/tail2kafka $CFGDIR || exit $?

sleep 1
$BUILDDIR/tail2kafka_blackbox

echo "WAIT kafka2file ... "; sleep 20
kill $KAFKA2FILE_ID

if [ ! -f $OLDFILE ]; then
  echo "kafka2file rotate error, expect $OLDFILE"
  exit 1
fi

NUM=$(wc -l $OLDFILE | cut -d' ' -f 1)
if [ "$NUM" != 200 ]; then
  echo "line of $OLDFILE is $NUM should be 200"
  exit 1
fi

# kafka2file
# current/last file was deleted after kill
# if current/last file exists, start failed
# current/last appear at the same time
# lost message plus.pingback.storage.1_2018-01-31_05-07-00 plus.pingback.storage.1_2018-01-31_05-20-00.current
