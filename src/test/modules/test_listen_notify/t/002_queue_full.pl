use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use PostgreSQL::Test::BackgroundPsql;
use Test::More;

# Verify that NOTIFY errors when the notification queue reaches the configured
# maximum page distance. Use a small max_notify_queue_pages so we can reach
# the limit quickly. With fixed-size compact entries, the last slot on the page
# cannot be used because advancing would require preparing the next page, which
# exceeds the allowed window.

my $node = PostgreSQL::Test::Cluster->new('t_queue_full');
$node->init;
$node->append_conf('postgresql.conf', qq{
max_notify_queue_pages = 64
fsync = off
synchronous_commit = off
full_page_writes = off
autovacuum = off
});
$node->start;

# Create a listener that registers and then stays in a transaction so it does
# not process incoming notifications, preventing the queue tail from advancing.
my $listener = $node->background_psql('postgres');
$listener->query_safe('LISTEN tap_queue_full');
$listener->query_safe('BEGIN');

my $sender = $node->background_psql('postgres', on_error_stop => 0);
my $full_seen = 0;
my $stderr_msg = '';

# Fill using moderate chunks to reach the boundary quickly.
my $chunk_sql = ("NOTIFY tap_queue_full, 'x';\n" x 500);
for my $iter (1..200) {
    last if $full_seen;
    my ($out, $errflag) = $sender->query($chunk_sql);
    my $errtxt = $sender->{stderr};
    if ($errtxt =~ /(asynchronous notification queue is full[^\n]*)/i) {
        $stderr_msg = $1;
        $full_seen = 1;
        last;
    }
}

ok($full_seen, 'NOTIFY fails once queue reaches configured maximum');
like($stderr_msg, qr/asynchronous notification queue is full/i,
     'error message mentions full NOTIFY queue');

# Now verify concurrent attempts also fail: create several senders and have
# each issue exactly one NOTIFY; all must be rejected.
my $n_concurrent = 4;
my @senders;
for (1..$n_concurrent) {
    push @senders, $node->background_psql('postgres', on_error_stop => 0);
}

my $all_failed = 1;
for my $s (@senders, $sender) {
    my ($out, $errflag) = $s->query("NOTIFY tap_queue_full, 'x'");
    my $errtxt = $s->{stderr};
    $all_failed &&= ($errflag && $errtxt =~ /asynchronous notification queue is full/i);
}

ok($all_failed, 'all concurrent NOTIFY attempts are rejected at boundary');

# Cleanup sessions and node
for my $s (@senders) {
    $s->quit();
}
$sender->quit();
$listener->query('ROLLBACK');
$listener->quit();
$node->stop('fast');

done_testing();
