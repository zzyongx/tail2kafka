#!/bin/bash -x

if [ "$NOTIFY_FILE" == "" ]; then
  echo "NOTIFY_FILE is required"
  exit 1
fi

if [ "$NOTIFY_FILESIZE" == "" ]; then
  echo "NOTIFY_FILESIZE is required"
  exit 1
fi

DT=$(date +%F_%H-%M-%S)

DIR=$(dirname $NOTIFY_FILE)
for f in $(ls $DIR/* -t | tail -n +7); do
  size=$(stat -c '%s' $f)
  echo $DT rm $f size $size
  rm -f $f
done
