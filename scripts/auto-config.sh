#!/bin/bash

SYSCONFIG=/etc/sysconfig/tail2kafka
test -f $SYSCONFIG && source $SYSCONFIG

if [ $# = 2 ]; then
  if [ -f $1 ]; then
    source $1
  else
    echo "$1 is not a file"
    exit 1
  fi
fi

log()
{
  local event="$1"
  local error="$2"

  local dt=$(date +%F_%H-%M-%S)
  echo "$dt $event $error"

  if [ "$PINGBACKURL" != "" ]; then
    error=$(echo $error | sed -e 's| |%20|g')
    curl -Ss "$PINGBACKURL?event=$event&product=$PRODUCT&error=$error" -o /dev/null
  fi
}

try_start()
{
  if [ "$BIN" != "" ] && [ -x $BIN ]; then
    log TRY_START "tail2kafka is not running, try start"
    $BIN $ETCDIR
    return 0
  else
    log RUNNING_ERROR "tail2kafka is not running"
    return 1
  fi
}

if [ "$ETCDIR" = "" ]; then
  log CONFIG_ERROR "param ETCDIR is required"
  exit 1
fi

if [ "$PIDFILE" = "" ]; then
  log CONFIG_ERROR "param PIDFILE is required"
  exit 1
fi

if [ "$PRODUCT" = "" ]; then
  log CONFIG_ERROR "param PRODUCT is required"
  exit 1
fi

if [ "$CONFIGURL" = "" ]; then
  log CONFIG_ERROR "param CONFIGURL is required"
  exit 1
fi

LIBDIR=${LIBDIR:-/tmp/tail2kafka}
mkdir -p $LIBDIR

find $LIBDIR -name "$PRODUCT.*" -type f -mtime +2

META=$(curl -Ssf "$CONFIGURL/$PRODUCT/meta")
if [ $? != 0 ]; then
  log SYNC_ERROR "curl $CONFIGURL/$PRODUCT/meta error"
  exit 1
fi

VER=$(echo $META | cut -d'-' -f1)
MD5=$(echo $META | cut -d'-' -f2)

test -f $LIBDIR/version && OLDVER=$(cat $LIBDIR/version)
if [ "$VER" = "$OLDVER" ]; then
  log VERSION_OK "version $VER has not changed"

  RET=0
  if [ ! -f $PIDFILE ] || [ ! -d /proc/$(cat $PIDFILE) ]; then
    try_start
    RET=$?
  fi
  exit $RET
fi

TGZ=$LIBDIR/$PRODUCT-$VER.tar.gz
curl -Ssf "$CONFIGURL/$PRODUCT/${PRODUCT}-${VER}.tar.gz" -o $TGZ
if [ $? != 0 ]; then
  log SYNC_ERROR "curl $CONFIGURL/$PRODUCT/${PRODUCT}-${VER}.tar.gz error"
  exit 1
fi

TGZMD5=$(md5sum $TGZ | cut -d' ' -f1)
if [ "$MD5" != "$TGZMD5" ]; then
  log SIGN_ERROR "config md5 error"
  exit 1
fi

cd $LIBDIR
tar xzf $TGZ
if [ ! -d $LIBDIR/$PRODUCT-$VER ]; then
  log TGZ_ERROR "BAD $TGZ, no $LIBDIR/$PRODUCT-$VER found"
  exit 1
fi

mv $ETCDIR $LIBDIR/$PRODUCT.$OLDVER.$(date +"%F_%H-%M-%S")
mv $LIBDIR/$PRODUCT-$VER $ETCDIR

RET=0
if [ -f $PIDFILE ] && [ -d /proc/$(cat $PIDFILE) ]; then
  PID=$(cat $PIDFILE)
  $CHILDPID=$(pgrep -P$PID)
  kill -HUP $PID

  RET=1
  for i in `seq 1 10`; do
    sleep 1
    NEW_CHILDPID=$(pgrep -P$PID)
    if [ "$NEW_CHILDPID" != "$CHILDPID" ]; then
      RET=0
      break
    fi
  done

  if [ "$RET" = 0 ]; then
    echo "$VER" > $LIBDIR/version
    log UPGRADE_OK "upgrade config from $OLDVER to $VER"
  else
    log UPGRADE_ERROR "upgrade config from $OLDVER to $VER, reload failed"
  fi
else
  try_start
  RET=$?
fi

exit $RET

# PRODUCT=_PRODUCT_ VER=_VER_
# mkdir $PRODUCT-$VER; cp *.lua $PRODUCT-$VER
# tar czf $PRODUCT-$VER.tar.gz $PRODUCT-$VER; MD5=$(md5sum $PRODUCT-$VER.tar.gz | cut -d' ' -f1); echo "$VER-$MD5" > meta
