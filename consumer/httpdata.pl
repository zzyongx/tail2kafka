#!/usr/bin/perl

use strict;
use warnings;
use Time::Local;
use FindBin qw($Bin);
use LWP;
use JSON::PP qw(decode_json encode_json);

my $api   = shift or usage();
my $start = shift || yesterday();
my $end   = shift || $start;

$start = str2time($start);
$end   = str2time($end);
while ($start <= $end) {
  my $day = time2str($start);
  my $rsp = LWP::UserAgent->new()->get("$api?date=$day");
  $rsp->is_success or die "$api?date=$day error";
  my $json = decode_json($rsp->content);

  my %total = (
    appid  => "total",
    reqnum => 0,
    qps    => 0,
    disk   => 0,
  );

  $day = time2str2($start);

  foreach my $o (@$json) {
    delete $o->{date};
    delete $o->{id};
    delete $o->{ori_appid};
    delete $o->{ol_appid};

    my $data = encode_json({cluster => $o});
    print "cluster $day ", $o->{appid}, " $data\n";

    $total{qps}    += $o->{qps};
    $total{reqnum} += $o->{reqnum};
    $total{disk}   += $o->{disk};

    if (exists $o->{bw}) {
      $total{bw} = (exists $total{bw}) ? $total{bw} + $o->{bw} : $o->{bw};
    }
    if (exists $o->{bwcdn}) {
      $total{bwcdn} = (exists $total{bwcdn}) ? $total{bwcdn} + $o->{bwcdn} : $o->{bwcdn};
    }
  }
  my $data = encode_json({cluster => \%total});
  print "cluster $day total $data\n";

  $start += 86400;
}

sub time2str {
  my $time = shift;
  my @v = localtime($time);
  return sprintf('%04d%02d%02d', $v[5]+1900, $v[4]+1, $v[3]);
}

sub time2str2 {
  my $time = shift;
  my @v = localtime($time);
  return sprintf('%04d-%02d-%02d', $v[5]+1900, $v[4]+1, $v[3]);
}

sub str2time {
  my $s = shift;
  $s =~ /^(\d{4})-(\d{2})-(\d{2})/;
  return timelocal(0, 0, 0, $3, $2-1, $1-1900);
}

sub yesterday {
  my @v = localtime(time - 86400);
  return sprintf('%04d-%02d-%02d', $v[5]+1900, $v[4]+1, $v[3]);
}

sub usage {
  print "$0 api [start] [end]\n";
  exit(0);
}
