#!/bin/bash -x

if [ "$NOTIFY_TOPIC" = "" ]; then
  echo "NOTIFY_TOPIC is required"
  exit 1
fi

if [ "$NOTIFY_FILE" = "" ]; then
  echo "NOTIFY_FILE is required"
  exit 1
fi

if [ "$NOTIFY_FILESIZE" = "" ]; then
  echo "NOTIFY_FILESIZE is required"
  exit 1
fi

test -f /etc/sysconfig/tail2kafka && source /etc/sysconfig/tail2kafka

if [ "$NOTIFY_ORIFILE" != "" ] && [ "$PINGBACKURL" != "" ] && [ "$NOTIFY_FILEMD5" != "" ]; then
  curl -Ss "$PINGBACKURL?event=CATNULL&file=$NOTIFY_ORIFILE&size=$NOTIFY_FILESIZE&md5=$NOTIFY_FILEMD5&topic=$NOTIFY_TOPIC"
fi

DT=$(date +%F_%H-%M-%S)

DIR=$(dirname $NOTIFY_FILE)
for f in $(ls $DIR/* -t | tail -n +7); do
  size=$(stat -c '%s' $f)
  echo $DT rm $f size $size
  rm -f $f
done

if [ "$BLACKBOXTEST_OUTFILE" != "" ]; then
  printenv | grep NOTIFY_ > $BLACKBOXTEST_OUTFILE
fi
