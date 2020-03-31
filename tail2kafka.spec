Name:      tail2kafka
Version:   2.2.1
Release:   1
Summary:   stream file data to kafka/stream kafka data to file
Group:     tail2kafka
License:   Apache2
Source0:   tail2kafka-2.2.1.tar.gz
BuildRoot: /var/tmp/tail2kafka
BuildRequires: libcurl-devel >= 7.19.7
BuildRequires: openssl-devel >= 1.0.1e-30
Requires: libcurl >= 7.19.7
Requires: openssl >= 1.0.1e-30
AutoReqProv: no

%description
use inotify stream file data to kafka
consume kafka data to file

%prep
%setup -q

%build
make clean
make

%install
mkdir -p $RPM_BUILD_ROOT/usr/local/bin
cp build/tail2kafka  $RPM_BUILD_ROOT/usr/local/bin
cp build/kafka2file  $RPM_BUILD_ROOT/usr/local/bin
cp scripts/auto-upgrade.sh $RPM_BUILD_ROOT/usr/local/bin/tail2kafka-auto-upgrade.sh

mkdir -p $RPM_BUILD_ROOT/etc/cron.d
cp scripts/tail2kafka.cron $RPM_BUILD_ROOT/etc/cron.d/tail2kafka

mkdir -p $RPM_BUILD_ROOT/etc/rc.d/init.d
cp scripts/tail2kafka.init $RPM_BUILD_ROOT/etc/rc.d/init.d/tail2kafka

mkdir -p $RPM_BUILD_ROOT/etc/sysconfig
cp scripts/tail2kafka.config $RPM_BUILD_ROOT/etc/sysconfig/tail2kafka

mkdir -p $RPM_BUILD_ROOT/etc/tail2kafka
mkdir -p $RPM_BUILD_ROOT/usr/share/tail2kafka/etc
cp blackboxtest/tail2kafka/*.lua  $RPM_BUILD_ROOT/usr/share/tail2kafka/etc

mkdir -p $RPM_BUILD_ROOT/etc/kafka2file
mkdir -p $RPM_BUILD_ROOT/usr/share/kafka2file/etc
cp blackboxtest/kafka2file/*.lua $RPM_BUILD_ROOT/usr/share/kafka2file/etc

mkdir -p $RPM_BUILD_ROOT/var/lib/tail2kafka
mkdir -p $RPM_BUILD_ROOT/var/log/tail2kafka

%files
%defattr(-,root,root)
/usr/local/bin
/usr/share/tail2kafka/etc
/usr/share/kafka2file/etc

/etc/tail2kafka
/etc/kafka2file
/etc/rc.d/init.d
/etc/cron.d

/var/lib/tail2kafka
/var/log/tail2kafka

%config(noreplace) /etc/sysconfig/tail2kafka

%clean
rm -rf $RPM_BUILD_ROOT

%post
ln -sf ../init.d/tail2kafka /etc/rc.d/rc3.d/S88tail2kafka

%changelog
* Mon Mar 18 2019 zzyongx <iamzhengzhiyong@gmail.com> -2.1.2-1
- Changes: tail2es add flow control
- Changes: tail2es use multi-thread

* Thu Feb 21 2019 zzyongx <iamzhengzhiyong@gmail.com> -2.1.1-1
- Feature: tail2es add basic auth
- Bugfix: make cleantransformEsDocNginxLog

* Mon Jan 28 2019 zzyongx <iamzhengzhiyong@gmail.com> -2.1.0-1
- Feature: add tail2es

* Wed Apr 18 2018 zzyongx <iamzhengzhiyong@gmail.com> -2.0.0-19
- bugfix: turn off withhost flag leading to a dead cycle

* Wed Apr 11 2018 zzyongx <iamzhengzhiyong@gmail.com> -2.0.0-18
- bugfix: reset cnfctx when respawn
- bugfix: when rotate, record the file corresponding th fd, so as not to lose data when the process crashs
- changes: exit when trap in KafkaCtx::produce
- changes: support random partitioner

* Sun Apr  8 2018 zzyongx <iamzhengzhiyong@gmail.com> -2.0.0-15
- bugfix: checkRotate should use tail2kafka(NIL)\'s return value

* Wed Apr  4 2018 zzyongx <iamzhengzhiyong@gmail.com> -2.0.0-14
- changes: refactor kafka block
- bugfix: tagRotate

* Tue Feb 27 2018 zzyongx <iamzhengzhiyong@gmail.com> -2.0.0-10
- bugfix: history file may lost

* Thu Feb 22 2018 zzyongx <iamzhengzhiyong@gmail.com> -2.0.0-9
- Feature: support md5 checksum

* Wed Feb 14 2018 zzyongx <iamzhengzhiyong@gmail.com> -2.0.0-8
- Feature: first release
