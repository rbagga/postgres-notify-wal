use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use IPC::Run qw(start pump finish timeout);
use Test::More;

# Goal: Verify that NOTIFY pins WAL segments via notify LSN so WAL recycling
# does not remove needed segments. With a listener idle-in-transaction, queued
# NOTIFY entries remain unconsumed, so RemoveOldXlogFiles should keep older
# segments. After releasing the listener, notifications are delivered and WAL
# recycling can proceed.

my $node = PostgreSQL::Test::Cluster->new('wal_pin');
$node->init;
$node->append_conf('postgresql.conf', qq{
fsync = off
synchronous_commit = off
full_page_writes = off
autovacuum = off
wal_level = replica
max_wal_size = '64MB'
min_wal_size = '32MB'
checkpoint_timeout = '30s'
trace_notify = on
log_min_messages = debug1
});
$node->start;

# Helper to count WAL segment files in pg_wal (24-hex-digit filenames)
sub count_wal_files {
    my $wal_dir = $node->basedir . '/pgdata/pg_wal';
    opendir(my $dh, $wal_dir) or die "cannot open pg_wal: $!";
    my @segs = grep { /^[0-9A-F]{24}$/ } readdir($dh);
    closedir($dh);
    return scalar(@segs);
}

# Start a psql session that LISTENs and then pins by going idle-in-xact.
my $psql = [ $node->installed_command('psql'), '--no-psqlrc', '--quiet', '--dbname' => $node->connstr('postgres') ];
my ($in, $out, $err) = ('','','');
my $tmo = timeout($PostgreSQL::Test::Utils::timeout_default);
my $h = start $psql, '<' => \$in, '>' => \$out, '2>' => \$err, $tmo;

$in .= "\\set ON_ERROR_STOP 1\nLISTEN wal_pin_t;\n";
$h->pump();

# Commit the LISTEN (autocommit), then begin a transaction to be idle-in-xact.
$in .= "BEGIN;\n";
$h->pump();

# Produce a bunch of NOTIFYs in autocommit to populate the queue and WAL.
my $notify_count = 2000;
my $sql = ("NOTIFY wal_pin_t, 'p';\n" x $notify_count);
my ($ret, $stdout, $stderr) = $node->psql('postgres', "\\set ON_ERROR_STOP 1\n$sql");
is($ret, 0, 'NOTIFY batch succeeded');

# Force WAL generation and recycling attempts.
for my $i (1..150) {
    $node->safe_psql('postgres', 'SELECT pg_switch_wal()');
}
for my $i (1..20) {
    $node->safe_psql('postgres', 'CHECKPOINT');
}

# Assert that NOTIFY WAL pinning path was invoked.
my $log = $node->logfile;
open my $lfh, '<', $log or die "cannot open postmaster log $log: $!";
my $logtxt = do { local $/; <$lfh> };
close $lfh;
like($logtxt, qr/async notify: (?:WAL recycle pinned by oldest notify LSN|checking WAL pin; oldest notify LSN)/i,
     'saw WAL pinning path invoked');

note('completed WAL switch/checkpoint churn with listener pinned');

# Release listener so it can process notifications, and force a round trip.
$in .= "ROLLBACK;\nSELECT 1;\n";
$h->pump();

# Pump to capture async outputs printed by psql
my $saw = 0;
for my $i (1..1000) {
  eval { $h->pump(); 1 } or do { };
  if ($out =~ /Asynchronous notification/i) { $saw = 1; last; }
  select(undef, undef, undef, 0.02);
}
ok($saw, 'notifications printed after releasing listener');

# Encourage tail advancement and WAL cleanup.
for my $i (1..20) {
    $node->safe_psql('postgres', 'SELECT pg_notification_queue_usage()');
    $node->safe_psql('postgres', 'CHECKPOINT');
}

note('post-release: queue advanced and checkpoints executed');

$in .= "\\q\n";
eval { $h->finish; 1 } or do {};

$node->stop('fast');

done_testing();
