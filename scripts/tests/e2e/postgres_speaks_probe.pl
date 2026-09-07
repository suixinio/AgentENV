#!/usr/bin/perl
# Whether whatever answers HOST:PORT speaks the postgres wire protocol.
#
# `postgres_broker_probe.pl` beside this one runs a whole session and needs a
# credential, a database and a query; this one only asks whether the peer says
# anything at all to a startup packet, and it always exits 0 so the caller
# reads the word rather than a status.
#
# Usage: postgres_speaks_probe.pl HOST PORT [TIMEOUT_SECONDS]
# Prints exactly one of:
#   bytes=<n> tag=<T>  the peer answered; only a postgres speaker does
#   silent             it accepted the connection and said nothing before the
#                      deadline, or reset it
#   eof                it closed cleanly without answering
#   connect_failed     nothing accepted the connection
use strict;
use warnings;
use IO::Socket::INET;

my ($host, $port, $timeout) = @ARGV;
$timeout = 3 unless defined $timeout && $timeout > 0;

my $sock = IO::Socket::INET->new(
    PeerAddr => $host, PeerPort => $port, Proto => 'tcp', Timeout => $timeout,
);
unless ($sock) { print "connect_failed\n"; exit 0 }
binmode($sock);

# A protocol 3.0 startup packet. The parameters are placeholders: the broker
# authenticates upstream itself and never sends these on.
my $params = pack('N', 196608);
for my $pair (['user', 'probe'], ['database', 'probe'], ['application_name', 'speaks']) {
    $params .= $pair->[0] . "\0" . $pair->[1] . "\0";
}
$params .= "\0";
syswrite($sock, pack('N', length($params) + 4) . $params);

my $buf = '';
eval {
    local $SIG{ALRM} = sub { die "timeout\n" };
    alarm($timeout);
    my $got = sysread($sock, $buf, 4096);
    alarm(0);
    die "timeout\n" unless defined $got;
    if ($got == 0) { print "eof\n"; exit 0 }
    printf "bytes=%d tag=%s\n", $got, substr($buf, 0, 1);
    exit 0;
};
print "silent\n";
exit 0;
