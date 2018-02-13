#!/bin/bash

SYSCONFIG=/etc/sysconfig/tail2kafka
test -f $SYSCONFIG && source $SYSCONFIG

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

download_config()
{
  local oldver="$1"
  local ver="$2"
  local md5="$3"

  local tgz=$LIBDIR/$PRODUCT-$ver.tar.gz
  curl -Ssf "$CONFIGURL/$PRODUCT/${PRODUCT}-${ver}.tar.gz" -o $tgz
  if [ $? != 0 ]; then
    log CONFIG_SYNC_ERROR "curl $CONFIGURL/$PRODUCT/${PRODUCT}-${ver}.tar.gz error"
    exit 1
  fi

  tgzmd5=$(md5sum $tgz | cut -d' ' -f1)
  if [ "$md5" != "$tgzmd5" ]; then
    log CONFIG_SIGN_ERROR "config md5 error"
    exit 1
  fi

  cd $LIBDIR
  tar xzf $tgz
  if [ ! -d $LIBDIR/$PRODUCT-$ver ]; then
    log CONFIG_TGZ_ERROR "BAD $tgz, no $LIBDIR/$PRODUCT-$ver found"
    exit 1
  fi

  mv $ETCDIR $LIBDIR/$PRODUCT.$oldver.$(date +"%F_%H-%M-%S")
  mv $LIBDIR/$PRODUCT-$ver $ETCDIR
}

try_reload()
{
  local oldver="$1"
  local ver="$2"

  local pid=$(cat $PIDFILE)
  child_pid=$(pgrep -P$pid)
  kill -HUP $pid

  ret=1
  for i in `seq 1 10`; do
    sleep 1
    new_child_pid=$(pgrep -P $pid)
    if [ "$new_child_pid" != "$child_pid" ]; then
      ret=0
      break
    fi
  done

  if [ "$ret" = 0 ]; then
    echo "$ver" > $LIBDIR/version
    log UPGRADE_CONFIG_OK "upgrade config from $oldver to $ver"
  else
    log UPGRADE_CONFIG_ERROR "upgrade config from $oldver to $ver, reload failed"
    exit 1
  fi
}

auto_config()
{
  if [ "$CONFIGURL" = "" ]; then
    log CONFIG_ERROR "param CONFIGURL is required"
    exit 1
  fi

  LIBDIR=${LIBDIR:-/tmp/tail2kafka}

  mkdir -p $LIBDIR
  find $LIBDIR -name "$PRODUCT.*" -mtime +2 -exec rm -r "{}" \;

  local meta # WARN declare and assign must be separated
  meta=$(curl -Ssf "$CONFIGURL/$PRODUCT/meta")
  if [ $? != 0 ]; then
    log CONFIG_SYNC_ERROR "curl $CONFIGURL/$PRODUCT/meta error"
    exit 1
  fi

  local ver=$(echo $meta | cut -d'-' -f1)
  local md5=$(echo $meta | cut -d'-' -f2)

  local oldver="NIL"
  test -f $LIBDIR/version && oldver=$(cat $LIBDIR/version)
  if [ "$ver" = "$oldver" ]; then
    log CONFIG_VERSION_OK "version $ver has not changed"

    if [ ! -f $PIDFILE ] || [ ! -d /proc/$(cat $PIDFILE) ]; then
      try_start
    fi
    exit 0
  fi

  download_config $oldver $ver $md5

  if [ -f $PIDFILE ] && [ -d /proc/$(cat $PIDFILE) ]; then
    try_reload $oldver $ver
    ret=$?
  else
    try_start
    ret=$?
  fi
  exit $ret
}

get_rpm_version()
{
  local host_id="$1"

  local ulist;
  ulist=$(curl -Ssf $RPMURL/version)
  if [ $? != 0 ]; then
    log UPGRADE_SYNC_ERROR "curl $RPMURL/version error"
    exit 1
  fi

  local version=""
  for line in $ulist; do
    local id=$(echo $line | cut -d'=' -f1)
    local ver=$(echo $line | cut -d'=' -f2)
    if echo $host_id | grep -Pq $id; then
      version=$ver
      break
    elif [ "$id" = "*" ]; then
      version=$ver
      break
    fi
  done

  if [ "$version" = "" ] || rpm -q tail2kafka | grep $version; then
    log UPGRADE_RPM_OK "version $version has not changed"
    exit 0
  fi

  _RET=$version
  return 0
}

upgrade_rpm()
{
  local version="$1"

  rpm -Uvh $RPMURL/tail2kafka-$version.x86_64.rpm
  if [ $? != 0 ]; then
    log RPM_INSTALL_ERROR "install $version error"
    exit 1
  fi

  kill $(cat $PIDFILE)
  sleep 5

  $BIN $ETCDIR
  log RPM_UPGRADE_OK "tail2kafka upgrade to $version"
}

auto_rpm()
{
  if [ "$RPMURL" = "" ]; then
    log CONFIG_ERROR "param RPMURL is required"
    exit 1
  fi

  if [ "$BIN" = "" ]; then
    log CONFIG_ERROR "param BIN is required"
    exit 1
  fi

  local host_id=${HOSTID:-$PRODUCT};
  get_rpm_version $host_id
  local version=$_RET

  upgrade_rpm $version
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

ACTION=$1

if [ "$ACTION" = "config" ]; then
  auto_config
elif [ "$ACTION" = "rpm" ]; then
  auto_rpm
else
  echo "$0 config|rpm"
  exit 1
fi

# PRODUCT=_PRODUCT_ VER=_VER_
# mkdir $PRODUCT-$VER; cp *.lua $PRODUCT-$VER
# tar czf $PRODUCT-$VER.tar.gz $PRODUCT-$VER; MD5=$(md5sum $PRODUCT-$VER.tar.gz | cut -d' ' -f1); echo "$VER-$MD5" > meta
