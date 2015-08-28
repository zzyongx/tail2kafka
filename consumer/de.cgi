#!/usr/bin/perl

use strict;
use warnings;
use Time::Local;
use FindBin qw($Bin);
use LWP;
use JSON::PP qw(decode_json);

# QUERY_STRING="topic=yuntu-app-tair&id=yuntu&start=2015-04-21T16:00:00&end=2015-04-21T16:30:00" /var/www/cgi-bin/de.cgi

my $Cmd = "$Bin/cassandra2aggregate";
my $Ca  = "10.134.72.118";

my $query = $ENV{QUERY_STRING};
$query =~ s/%(.{2})/chr(hex($1))/ge;

my %param = map {my ($k, $v) = split "=", $_; $k => $v} split "&", $query;

print "Content-Type: text/event-stream\n";
print "Cache-Control: no-cache\n";
print "Access-Control-Allow-Origin: *\n";
print "\n";

print "retry: 10000\n";

if ($param{end} eq "forever")  {
  my ($time, $unit) = str2time($param{start});

  while (1) {
    while ($time + ($unit == 1 ? 5 : 60) > time) {
      sleep($unit);
    }
    my $start = time2str($time, $unit);
    my $cmd = join(" ", $Cmd, $Ca, $param{topic}, $param{id}, $start, $start, "asc", "all");
    my $out = `$cmd`;
    print scalar localtime, " ", $cmd, "\n";
    print $out unless ($out eq "");
    $time += $unit;
  }
} else {
  my $cmd = join(" ", $Cmd, $Ca,
                 $param{topic}, $param{id}, $param{start}, $param{end},
                 $param{order} || "desc", $param{mode} || "all");
  exec($cmd);
}

sub time2str {
  my ($time, $unit) = @_;
  my @v = localtime($time);
  if ($unit == 1) {
    return sprintf("%04d-%02d-%02dT%02d:%02d:%02d", $v[5]+1900, $v[4]+1, $v[3], $v[2], $v[1], $v[0]);
  } else {
    return sprintf("%04d-%02d-%02dT%02d:%02d", $v[5]+1900, $v[4]+1, $v[3], $v[2], $v[1]);
  }
}

sub str2time {
  my $s = shift;
  $s =~ /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2})(:(\d{2}))?/;
  my $unit = 1;
  my $second;
  if (! defined $7) {
    $unit = 60;
    $second = 0;
  } else {
    $second = $7;
  }
  return (timelocal($second, $5, $4, $3, $2-1, $1-1900), $unit);
}
