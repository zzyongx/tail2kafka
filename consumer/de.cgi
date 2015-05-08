#!/usr/bin/perl

use strict;
use warnings;

# QUERY_STRING="topic=yuntu-app-tair&id=yuntu&start=2015-04-21T16:00:00&end=2015-04-21T16:30:00" /var/www/cgi-bin/de.cgi

my $query = $ENV{QUERY_STRING};
$query =~ s/%(.{2})/chr(hex($1))/ge;

my %param = map {my ($k, $v) = split "=", $_; $k => $v} split "&", $query;

print "Content-Type: text/event-stream\n";
print "Cache-Control: no-cache\n";
print "Access-Control-Allow-Origin: *\n";

print "retry: 10000\n";
exec("/var/www/cgi-bin/cassandra2aggregate", "127.0.0.1",
     $param{topic}, $param{id}, $param{start}, $param{end},
     $param{order} || "desc", $param{dataset} || "samp");
