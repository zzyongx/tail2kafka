#!/bin/bash

BINDIR=$(readlink -e $(dirname "${BASH_SOURCE[0]}"))
CFGDIR="$BINDIR/loadtest"
PIDF=/var/run/tail2kafka.pid
LIBDIR=/var/lib/tail2kafka

DATADIR=${DATADIR:-$HOME}
T2KDIR=$DATADIR/data
K2FDIR=$DATADIR/data/kafka2file

test -f $BINDIR/../ENV.sh && source $BINDIR/../ENV.sh
KAFKASERVER=${KAFKASERVER:-"localhost:9092"}
ACK=${ACK:-1}
cp $CFGDIR/main.lua $CFGDIR/main.lua.backup
cp $CFGDIR/linecopy.lua $CFGDIR/linecopy.lua.backup
sed -i -E "s|localhost:9092|$KAFKASERVER|g" $CFGDIR/main.lua
sed -i -E "s|_ACK_|$ACK|g" $CFGDIR/main.lua
sed -i -E "s|BIGLOG|$T2KDIR/big.log|g" $CFGDIR/linecopy.lua

mkdir -p $K2FDIR
find $K2FDIR -type f -delete

gen_bigdata()
{
  local name="$1"
  local loop="$2"
  local size="$3"
  local f="$4"
  perl -e "\$name='$name';\$loop=$loop;\$size=$size;" -e 'for $i (1 .. $loop) {print "$name $i ", "f" x int(rand($size)), "\n";}' >$f
}

(test -f $PIDF && test -d /proc/$(cat $PIDF)) && kill $(cat $PIDF); sleep 2
TOPIC=biglog
K2FPID=$K2FDIR/$TOPIC.0.lock
(test -f $K2FPID && test -d /proc/$(cat $K2FPID)) && kill $(cat $K2FPID); sleep 2;  kill -9 $(cat $K2FPID 2>/dev/null) 2>/dev/null

N=${LOADTEST_N:-500000}
SIZE=0

# prepare history file
rm -f $LIBDIR/{biglog.history,biglog.current}
for suffix in 2 1; do
  gen_bigdata "history.$suffix" $N 4096 $T2KDIR/big.log.history.$suffix
  echo "$T2KDIR/big.log.history.$suffix" >> $LIBDIR/biglog.history
  SIZE=$((SIZE + $(stat -c %s $T2KDIR/big.log.history.$suffix)))
done

echo "history file"
cat $LIBDIR/biglog.history

export BLACKBOXTEST_OUTFILE_TPL=$K2FDIR/catnull

find $K2FDIR -type f -delete
$BINDIR/../build/kafka2file $KAFKASERVER $TOPIC 0 offset-end $K2FDIR $BINDIR/../scripts/catnull.sh &
sleep 1
if [ ! -f $K2FPID ] || [ ! -d /proc/$(cat $K2FPID) ]; then
  echo "start kafka2file failed"
  exit 1
fi

START=$(date +%s)
$BINDIR/../build/tail2kafka $CFGDIR; sleep 2
if [ ! -f $PIDF ] || [ ! -d /proc/$(cat $PIDF) ]; then
  echo "start tail2kafka failed"
  exit 1
fi
mv $CFGDIR/main.lua.backup $CFGDIR/main.lua
mv $CFGDIR/linecopy.lua.backup $CFGDIR/linecopy.lua

gen_bigdata "current" $N 4096 $T2KDIR/big.log
SIZE=$((SIZE + $(stat -c %s $T2KDIR/big.log)))
mv $T2KDIR/big.log $T2KDIR/big.log.2

echo "$(date) current move to ${T2KDIR}/big.log.2"

CHILDPID=$(pgrep -P $(cat $PIDF))
while [ true ]; do
  FOPEN=$(ls -l /proc/$CHILDPID/fd | grep big.log | wc -l)
  if [ "$FOPEN" = 1 ] && ls -l /proc/$CHILDPID/fd | grep -qP 'big.log$'; then
    break
  else
    sleep 1
  fi
done

SPAN=$(($(date +%s) - START - 2))
echo "tail size $SIZE, time consumption ${SPAN}s"

for f in "big.log.history.2" "big.log.history.1" "big.log.2"; do
  test -f $BLACKBOXTEST_OUTFILE_TPL.$f || {
    echo "$f catnull notfound"
    exit 1
  }

  source $BLACKBOXTEST_OUTFILE_TPL.$f
  MD5ORIFILE=$(md5sum $T2KDIR/$f | cut -d' ' -f1)
  if [ $MD5ORIFILE != "$NOTIFY_FILEMD5" ]; then
    echo "$f md5error, expect $MD5ORIFILE, get $NOTIFY_FILEMD5"
    exit 1
  fi
done

N=2000000
for block in 4096 2048 1024 512; do
  START=$(date +%s)

  gen_bigdata "current" $N $block $T2KDIR/big.log

  sleep 5
  echo "$(date) current move to /search/odin/data/big.log.$block"
  mv $T2KDIR/big.log $T2KDIR/big.log.$block

  ROTATESPAN=$(($(date +%s) - START - 2))

  while [ true ]; do
    if [ -f $BLACKBOXTEST_OUTFILE_TPL.big.log.$block ]; then
      T2KSPAN=$(($(date +%s) - START - 2))
      READSPAN=$(/usr/bin/time -f %e cp /root/data/big.log.$block /dev/null 2>&1)   # put it in before md5 to avoid read cache

      source $BLACKBOXTEST_OUTFILE_TPL.big.log.$block
      if [ $(md5sum $T2KDIR/big.log.$block | cut -d' ' -f1) != "$NOTIFY_FILEMD5" ]; then
        echo "big.log.1 md5error"
        exit 1
      fi
      break
    else
      sleep 1
    fi
  done

  SIZE=$(stat -c %s $T2KDIR/big.log.$block)
  echo -n "tail size $SIZE, line $N, generation time consumption ${ROTATESPAN}s, "
  echo    "read time consumption ${READSPAN}s, tail2kafka time consumption in ${T2KSPAN}s"
done
