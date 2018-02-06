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

echo "WARN: YOU MUST KILL tail2kafka and kafka2file first, both may create topic automatic"

T2KDIR=logs
K2FDIR=kafka2filedir

echo "kill tail2kafka"
(test -f $PIDF && test -d /proc/$(cat $PIDF)) && kill $(cat $PIDF); sleep 2
echo "kill kafka2file"
for TOPIC in "basic" "filter" "grep" "aggregate" "transform"; do
  K2FPID=$K2FDIR/$TOPIC.0.lock
  (test -f $K2FPID && test -d /proc/$(cat $K2FPID)) && kill $(cat $K2FPID); sleep 2
done

find logs -type f -name "*.log" -delete

ZK=localhost:2181/kafka
# delete.topic.enable=true
cd /opt/kafka
for TOPIC in "basic" "filter" "grep" "aggregate" "transform"; do
  bin/kafka-topics.sh --delete --if-exists --zookeeper $ZK  --topic $TOPIC
  bin/kafka-topics.sh --create --zookeeper $ZK --replication-factor 1 --partitions 1 --topic $TOPIC
done
if bin/kafka-topics.sh --list --zookeeper $ZK | grep -e 'aggregate|basic|filter|grep|transform'; then
  echo "delete kafka topic error"
  exit 1
fi
cd -

OLDFILE=$K2FDIR/basic/zzyong_basic.log.old
test -d $K2FDIR || mkdir $K2FDIR
rm -f $K2FDIR/basic.0.offset $OLDFILE

K2FPID=$K2FDIR/basic.0.lock
$BUILDDIR/kafka2file 127.0.0.1:9092 basic 0 offset-end $K2FDIR &
sleep 5
if [ ! -f $K2FPID ] || [ ! -d /proc/$(cat $K2FPID) ]; then
  echo "start kafka2file failed"
  exit 1
fi

$BUILDDIR/tail2kafka $CFGDIR
if [ ! -f $PIDF ] || [ ! -d /proc/$(cat $PIDF) ]; then
  echo "start tail2kafka failed"
  exit 1;
fi

sleep 1
$BUILDDIR/tail2kafka_blackbox

echo "WAIT kafka2file ... "; sleep 20

if [ ! -f $OLDFILE ]; then
  echo "kafka2file rotate error, expect $OLDFILE"
  exit 1
fi

NUM=$(wc -l $OLDFILE | cut -d' ' -f 1)
if [ "$NUM" != 200 ]; then
  echo "line of $OLDFILE is $NUM should be 200"
  exit 1
fi

echo "$0 test ok"

# kafka2file
# current/last file was deleted after kill
# if current/last file exists, start failed
# current/last appear at the same time
# lost message plus.pingback.storage.1_2018-01-31_05-07-00 plus.pingback.storage.1_2018-01-31_05-20-00.current
