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

  if [ $PINGBACKURL != "" ]; then
    error=$(echo $error | sed -e 's| |%20|g')
    curl -Ss "$PINGBACKURL?event=$event&product=$PRODUCT&error=$error"
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

if [ ! -f $PIDFILE ]; then
  log CONFIG_ERROR "PIDFILE $PIDFILE is not a file"
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

META=$(curl -Ssf "$CONFIGURL/$PRODUCT/meta")
if [ $? != 0 ]; then
  log SYNC_ERROR "curl $CONFIGURL/$PRODUCT/meta error"
  exit 1
fi

VER=$(echo $META | cut -d'-' -f1)
MD5=$(echo $META | cut -d'-' -f2)

test -f $LIBDIR/version && OLDVER=$(cat $LIBDIR/version)
if [ "$VER" = "$OLDVER" ]; then
  log VERSION_OK "version $VER is not changed"
  exit 0
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
kill -HUP $(cat $PIDFILE)

echo "$VER" > $LIBDIR/version
log UPGRADE_OK "upgrade config from $OLDVER to $VER"

# tar czf _PRODUCT_-_VER_.tar.gz _PRODUCT_-_VER_
# MD5=$(md5sum _PRODUCT_-_VER_.tar.gz | cut -d' ' -f1); echo "_VER_-$MD5" > meta
