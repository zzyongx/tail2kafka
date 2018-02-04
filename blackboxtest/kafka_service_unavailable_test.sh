#!/bin/bash

BIN="${BASH_SOURCE[0]}"
BINDIR=$(readlink -e $(dirname $BIN))

CFGDIR="$BINDIR/tail2kafka"
PIDF=/var/run/tail2kafka.pid
BUILDDIR=$BINDIR/../build

if [ ! -d $CFGDIR ]; then
  echo "$CFGDIR NOT FOUND"
  echo "disable autoparti"
  echo "main.lua partition=0"
  echo "main.lua pidfile=$PIDF"
  exit 1
fi

UNBLOCK_KAFKA="iptables -D OUTPUT -p tcp --dport 9092 -j REJECT --reject-with tcp-reset"
BLOCK_KAFKA="iptables -A OUTPUT -p tcp --dport 9092 -j REJECT --reject-with tcp-reset"
$UNBLOCK_KAFKA

test -d logs || mkdir logs
find logs -type f -name "*.log" -delete

test -d kafka2file || mkdir kafka2file
find kafka2file -type f -delete

# delete.topic.enable=true
TOPIC="basic"
cd /opt/kafka
bin/kafka-topics.sh --delete --zookeeper localhost:2181 --topic $TOPIC
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic $TOPIC
cd -

OLDFILE=kafka2filedir/basic/zzyong_basic.log.old
$BUILDDIR/kafka2file 127.0.0.1:9092 basic 0 offset-end kafka2filedir &
KAFKA2FILE_PID=$!
if [ $? != 0 ]; then
  echo "start kafka2file failed"
  exit 1
fi

(test -f $PIDF && test -d /proc/$(cat $PIDF)) && kill $(cat $PIDF)
sleep 1
$BUILDDIR/tail2kafka $CFGDIR
if [ ! -f $PIDF ] || [ ! -d /proc/$(cat $PIDF) ]; then
  echo "start tail2kafka failed"
  exit 1;
fi

$BLOCK_KAFKA
sleep 1

NLINE=100000
LOGFILE=logs/basic.log
for i in `seq 1 $NLINE`; do
  echo "BASIC $i" >> $LOGFILE
done

echo "WAIT kafka2file ... "; sleep 20
kill $KAFKA2FILE_PID

if [ ! -f $OLDFILE ]; then
  echo "kafka2file rotate error, expect $OLDFILE"
  exit 1
fi

NUM=$(wc -l $OLDFILE | cut -d' ' -f 1)
if [ "$NUM" != 200 ]; then
  echo "line of $OLDFILE is $NUM should be 200"
  exit 1
fi

# qsize has bug, if set start, but without tail2kafka, should tail2kafka after init
