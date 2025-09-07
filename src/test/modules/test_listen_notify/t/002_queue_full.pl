use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use PostgreSQL::Test::BackgroundPsql;
use IPC::Run qw(start pump finish timeout);
use Test::More;

# Verify that NOTIFY errors when the notification queue reaches the configured
# maximum page distance. We force max_notify_queue_pages to 1 so we can reach
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

# Launch multiple concurrent psql senders that issue many autocommit NOTIFY
# statements. Stop as soon as one fails with the expected error.
my $n_senders = 4;
my @handles;
my @in; my @out; my @err;
my $full_seen = 0;
my $stderr_msg = '';

for my $i (0..$n_senders-1) {
    my @cmd = (
        $node->installed_command('psql'),
        '--no-psqlrc', '--no-align', '--tuples-only', '--quiet',
        '--dbname' => $node->connstr('postgres'),
        '--set' => 'ON_ERROR_STOP=1',
        '--file' => '-',
    );
    $in[$i] = '';
    $out[$i] = '';
    $err[$i] = '';
    my $tmo = timeout($PostgreSQL::Test::Utils::timeout_default);
    $handles[$i] = start \@cmd, '<' => \$in[$i], '>' => \$out[$i], '2>' => \$err[$i], $tmo;
}

my $chunk = ("NOTIFY tap_queue_full, 'x';\n" x 1000);
my $iterations = 0;
ITER: while (!$full_seen && $iterations < 1000) {
    $iterations++;
    for my $i (0..$#handles) {
        next if $full_seen; # break outer fast
        # feed a chunk to this sender
        $in[$i] .= $chunk;
        $handles[$i]->pump();
        # if this sender errored, capture message
        if ($err[$i] =~ /asynchronous notification queue is full/i) {
            $stderr_msg = $err[$i];
            $full_seen = 1;
            last ITER;
        }
    }
}

# Terminate all senders
for my $i (0..$#handles) {
    # try to finish gracefully; ignore errors
    eval { $handles[$i]->finish; 1 } or do {};
}

ok($full_seen, 'NOTIFY fails once queue reaches configured maximum');
like($stderr_msg, qr/asynchronous notification queue is full/i,
     'error message mentions full NOTIFY queue');

# Cleanup listener
$listener->query('ROLLBACK');
$listener->quit();

$node->stop('fast');

done_testing();
