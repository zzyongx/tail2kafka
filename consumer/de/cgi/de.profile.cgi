#!/usr/bin/perl

use strict;
use warnings;
use JSON::PP qw(encode_json);
use Data::Dumper;

my $DIR = "/search/de.profile";

if ($ENV{REQUEST_METHOD} eq "POST" || $ENV{REQUEST_METHOD} eq "PUT") {
  process_put();
} else {
  process_get();
}

sub process_put {
  my $user;
  my $cookie = $ENV{HTTP_COOKIE};
  if ($cookie && $cookie =~ /user=([^\s]+)/) {
    $user = $1;
  }
  if ($user && $user =~ /^[a-zA-Z0-9]+$/) {
    my $c = join("", <>);
    wfile("$DIR/$user.js", $c);
    print "Content-Type: text/plain\n";
    print "\nOK";
  } else {
    print "Status: 400\n";
    print "Content-Type: text/plain\n";
    print "\n";
  }
}

sub process_get {
  my $query = $ENV{QUERY_STRING};
  $query =~ s/%(.{2})/chr(hex($1))/ge;
  my %param = map {my ($k, $v) = split "=", $_; $k => $v} split "&", $query;

  my $user = $param{user};
  unless ($user) {
    my $cookie = $ENV{HTTP_COOKIE};
    if ($cookie && $cookie =~ /user=([^\s]+)/) {
      $user = $1;
    }
  }

  my $result;
  if ($user && $user =~ /^[a-zA-Z0-9_.-]+$/) {
    my $file = "$DIR/$user.js";
    if (exists $param{'topic'} && exists $param{'id'}
                 && exists $param{'attr'}) {
      $result = encode_json({
        topic => $param{'topic'}, id => $param{'id'},
        attr => [split /,/, $param{'attr'}], host => [split /,/, ($param{'host'} || "cluster")],
        attrs => []
      });
      wfile($file, $result);
    } elsif (-f $file) {
      open(my $fh, "<$DIR/$user.js");
      $result = join("", <$fh>);
      close($fh);
    }
  }

  if ($result) {
    print "Content-Type: text/javascript\n";
    print "Set-Cookie: user=$user Secure; HttpOnly\n";
    print "\n";
    print $result;
  } else {
    print "Status: 400\n";
    print "Content-Type: text/plain\n";
    print "\n";
    print "Usage: ?user=_user_&topic=_topic_&id=_id_&attr=_attr1_,_attr2_[&host=_host_]\n";
  }
}

sub wfile {
  my ($f, $c) = @_;
  open(my $fh, ">$f");
  print $fh $c;
  close($fh);
}
