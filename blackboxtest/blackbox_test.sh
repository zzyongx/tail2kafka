#!/bin/bash

BIN="${BASH_SOURCE[0]}"
BINDIR=$(readlink -e $(dirname $BIN))

CFGDIR="$BINDIR/tail2kafka"
PIDF=/var/run/tail2kafka.pid
LIBDIR=/var/lib/tail2kafka
BUILDDIR=$BINDIR/../build

if [ ! -d $CFGDIR ]; then
  echo "$CFGDIR NOT FOUND"
  echo "disable autoparti"
  echo "main.lua partition=0"
  echo "main.lua pidfile=$PIDF"
  exit 1
fi

# delete.topic.enable=true
test -f $BINDIR/../ENV.sh && source $BINDIR/../ENV.sh
KAFKAHOME=${KAFKAHOME:-"/opt/kafka"}
KAFKASERVER=${KAFKASERVER:-"localhost:9092"}
HOSTNAME=${HOSTNAME:-$(hostname)}

echo "WARN: YOU MUST KILL tail2kafka and kafka2file first, both may create topic automatic"

T2KDIR=logs
K2FDIR=kafka2filedir
TOPICS="basic basic2 filter grep aggregate transform match"

echo "kill tail2kafka"
(test -f $PIDF && test -d /proc/$(cat $PIDF)) && kill $(cat $PIDF); sleep 2
echo "kill kafka2file"
for TOPIC in $TOPICS; do
  K2FPID=$K2FDIR/$TOPIC.0.lock
  (test -f $K2FPID && test -d /proc/$(cat $K2FPID)) && kill $(cat $K2FPID); sleep 2;  kill -9 $(cat $K2FPID 2>/dev/null) 2>/dev/null
done

find $T2KDIR -type f -name "*.log" -delete

cd $KAFKAHOME
for TOPIC in $TOPICS; do
  bin/kafka-topics.sh --bootstrap-server $KAFKASERVER --delete --topic $TOPIC
done
bin/kafka-topics.sh --bootstrap-server $KAFKASERVER --list | egrep "$(echo $TOPICS | tr ' ' '|')" && {
  echo "$LINENO delete kafka topic error"
  exit 1
}
for TOPIC in $TOPICS; do
  bin/kafka-topics.sh --bootstrap-server $KAFKASERVER --create --replication-factor 1 --partitions 1 --topic $TOPIC
done

cd -

OLDFILE=$K2FDIR/basic/${HOSTNAME}_basic.log.old
test -d $K2FDIR || mkdir $K2FDIR
rm -f $K2FDIR/basic.0.offset $OLDFILE

export BLACKBOXTEST_OUTFILE=$K2FDIR/catnull

K2FPID=$K2FDIR/basic.0.lock
$BUILDDIR/kafka2file $KAFKASERVER basic 0 offset-end $K2FDIR $BUILDDIR/../scripts/catnull.sh &
sleep 5
if [ ! -f $K2FPID ] || [ ! -d /proc/$(cat $K2FPID) ]; then
  echo "start kafka2file failed"
  exit 1
fi

rm -rf $LIBDIR/*.history && rm -rf $LIBDIR/*.current
cp $CFGDIR/main.lua $CFGDIR/main.lua.backup
sed -i -E "s|localhost:9092|$KAFKASERVER|g" $CFGDIR/main.lua
$BUILDDIR/tail2kafka $CFGDIR; sleep 2
mv $CFGDIR/main.lua.backup $CFGDIR/main.lua

if [ ! -f $PIDF ] || [ ! -d /proc/$(cat $PIDF) ]; then
  echo "start tail2kafka failed"
  exit 1;
fi


sleep 1
export KAFKASERVER
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

source $BLACKBOXTEST_OUTFILE
if [ "$NOTIFY_TOPIC" = "" ] || [ $NOTIFY_TOPIC != "basic" ]; then
  echo "NOTIFY_TOPIC $NOTIFY_TOPIC != basic"
  exit 1
fi

if [ "$NOTIFY_ORIFILE" = "" ]; then
  echo "NOTIFY_ORIFIL is not set"
  exit 1
fi

if [ "$NOTIFY_FILE" = "" ]; then
  echo "NOTIFY_FILE is not set"
  exit 1
fi

if [ "$NOTIFY_FILESIZE" = "" ]; then
  echo "NOTIFY_FILESIZE is not set"
  exit 1
fi

if [ "$NOTIFY_FILEMD5" = "" ]; then
  echo "NOTIFY_FILEMD5 is not set"
  exit 1
fi

MD5FILE=$(md5sum $NOTIFY_FILE | cut -d' ' -f1)
MD5ORIFILE=$(md5sum $NOTIFY_ORIFILE | cut -d' ' -f1)
if [ "$MD5FILE" != "$MD5ORIFILE" ]; then
  echo "$NOTIFY_FILE and $NOTIFY_ORIFILE are not the same md5"
  exit 1
fi

echo "$0 test ok"
