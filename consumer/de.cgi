#!/usr/bin/perl

use strict;
use warnings;
use Time::Local;

# QUERY_STRING="topic=yuntu-app-tair&id=yuntu&start=2015-04-21T16:00:00&end=2015-04-21T16:30:00" /var/www/cgi-bin/de.cgi

my $Cmd = "/var/www/cgi-bin/cassandra2aggregate";
my $Ca  = "127.0.0.1";

my $query = $ENV{QUERY_STRING};
$query =~ s/%(.{2})/chr(hex($1))/ge;

my %param = map {my ($k, $v) = split "=", $_; $k => $v} split "&", $query;

print "Content-Type: text/event-stream\n";
print "Cache-Control: no-cache\n";
print "Access-Control-Allow-Origin: *\n";
print "\n";

print "retry: 10000\n";

if ($param{end} eq "forever")  {
  my $time = str2time($param{start});

  while (1) {
    my $start = time2str($time);
    my $cmd = join(" ", $Cmd, $Ca, $param{topic}, $param{id}, $start, $start, "asc", "all");
    my $out = `$cmd`;
    if ($out eq "") {
      sleep(1);
    } else {
      print $out;
      $time++;
    }
  }
} else {
  exec($Cmd, "127.0.0.1",
       $param{topic}, $param{id}, $param{start}, $param{end},
       $param{order} || "desc", $param{dataset} || "samp");
}

sub time2str {
  my @v = localtime(shift);
  return sprintf("%04d-%02d-%02dT%02d:%02d:%02d", $v[5]+1900, $v[4]+1, $v[3], $v[2], $v[1], $v[0]);
}

sub str2time {
  my $s = shift;
  $s =~ /(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})/;
  return timelocal($6, $5, $4, $3, $2-1, $1-1900);
}
