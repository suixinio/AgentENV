#!/usr/bin/perl
# The same probe as postgres_broker_probe.py, for a template that has perl and
# no python3 -- which is what the e2e driver's own base image ships. It
# connects to the brokered listener with a user and password that are
# placeholders, runs one query and prints the first row, so whatever account
# that row reports is the one the broker authenticated with.
#
# Usage: postgres_broker_probe.pl HOST PORT USER PASSWORD DATABASE SQL
# Prints the row, or `ERR:<sqlstate>:<message>` and exits non-zero.
use strict;
use warnings;
use IO::Socket::INET;

my ($host, $port, $user, $password, $database, $sql) = @ARGV;
# The password is never sent: the handler answers the guest's startup with
# AuthenticationOk once it has authenticated upstream itself.
$password = $password;

my $sock = IO::Socket::INET->new(
    PeerAddr => $host, PeerPort => $port, Proto => 'tcp', Timeout => 15,
) or do { print "ERR:connect:$!\n"; exit 1 };
binmode($sock);

sub read_exact {
    my ($n) = @_;
    my $buf = '';
    while (length($buf) < $n) {
        my $chunk;
        my $got = sysread($sock, $chunk, $n - length($buf));
        if (!defined $got || $got == 0) {
            print "ERR:closed:upstream closed the connection\n";
            exit 1;
        }
        $buf .= $chunk;
    }
    return $buf;
}

sub read_message {
    my $head = read_exact(5);
    my $len = unpack('N', substr($head, 1, 4));
    return (substr($head, 0, 1), read_exact($len - 4));
}

sub framed {
    my ($tag, $body) = @_;
    return $tag . pack('N', length($body) + 4) . $body;
}

my $params = pack('N', 196608);
for my $pair (['user', $user], ['database', $database], ['application_name', 'probe']) {
    $params .= $pair->[0] . "\0" . $pair->[1] . "\0";
}
$params .= "\0";
syswrite($sock, pack('N', length($params) + 4) . $params);

my @rows;
while (1) {
    my ($tag, $body) = read_message();
    if ($tag eq 'E') {
        my %f;
        for my $field (split /\0/, $body) {
            next unless length($field);
            $f{ substr($field, 0, 1) } = substr($field, 1);
        }
        printf "ERR:%s:%s\n", ($f{C} // '?'), ($f{M} // '?');
        exit 1;
    }
    if ($tag eq 'R') {
        my $kind = unpack('N', substr($body, 0, 4));
        if ($kind != 0) {
            print "ERR:auth:the broker asked the guest for authentication $kind\n";
            exit 1;
        }
        next;
    }
    if ($tag eq 'Z') {
        if (@rows) { print "$rows[0]\n"; exit 0 }
        syswrite($sock, framed('Q', $sql . "\0"));
        next;
    }
    if ($tag eq 'D') {
        my $count = unpack('n', substr($body, 0, 2));
        my $at = 2;
        my @values;
        for (1 .. $count) {
            my $size = unpack('l>', substr($body, $at, 4));
            $at += 4;
            if ($size < 0) { push @values, '' }
            else { push @values, substr($body, $at, $size); $at += $size }
        }
        push @rows, join('|', @values);
    }
}
