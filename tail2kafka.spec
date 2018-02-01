Name:      tail2kafka
Version:   2.0.0
Release:   1
Summary:   stream file data to kafka/stream kafka data to file
Group:     tail2kafka
License:   Apache2
Source0:   tail2kafka-2.0.0.tar.gz
BuildRoot: /var/tmp/tail2kafka
# BuildRequires:
# Requires:
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
cp tail2kafka  $RPM_BUILD_ROOT/usr/local/bin
cp kafka2file  $RPM_BUILD_ROOT/usr/local/bin

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
/etc/sysconfig

/var/lib/tail2kafka
/var/log/tail2kafka

%config

%clean
rm -rf $RPM_BUILD_ROOT
%post

%changelog
* Thu Jan 25 2018 zzyongx <iamzhengzhiyong@gmail.com> -2.0.0-1
- Feature: first release
