use strict;
use warnings;

use Test::More;
use Test::Exception;
use Test::Deep;

use Data::Dumper;
$Data::Dumper::Indent = 1;

my $warn = shift @ARGV;
unless ($warn) {
    close STDERR;
    open (STDERR, ">/dev/null");
    select (STDERR); $| = 1;
}

use constant DONE => 1;

use Log::Log4perl qw(:levels);
Log::Log4perl::init( \ q(

log4perl.category = DEBUG, Screen
log4perl.appender.Screen        = Log::Log4perl::Appender::ScreenColoredLevels
log4perl.appender.Screen.layout = Log::Log4perl::Layout::PatternLayout
log4perl.appender.Screen.layout.ConversionPattern = %d{HH:mm:ss} %p [%r] %H : %F %L %c - %m%n
                       ) );
my $log = Log::Log4perl->get_logger("ngb");
$log->level($warn ? $DEBUG : $ERROR); # one of DEBUG, INFO, WARN, ERROR, FATAL

use Net::Gearman::Blobbed;
$Net::Gearman::Blobbed::log = $log;

if (DONE) { # by foot
    my $blobbed = Net::Gearman::Blobbed->new;
    isa_ok( $blobbed, 'Net::Gearman::Blobbed' );

    use Net::Gearman::Blobbed::file;
    use Moose::Util qw(apply_all_roles);
    apply_all_roles($blobbed, 'Net::Gearman::Blobbed::file');
    use Moose::Util qw(does_role);
    ok(does_role($blobbed, 'Net::Gearman::Blobbed::file'), 'file role');

    is( $blobbed->tmpdir, undef, 'tmpdir yet undefined');
    $blobbed->tmpdir( '/tmp' );
    is( $blobbed->tmpdir, '/tmp', 'tmpdir defined');
}

if (DONE) { # abridged
    my $blobbed = Net::Gearman::Blobbed->new;

    use Net::Gearman::Blobbed::file;
    Net::Gearman::Blobbed::file->mk_fileable( $blobbed, tmpdir => '/tmp' );
    isa_ok( $blobbed, 'Net::Gearman::Blobbed' );
    ok(does_role($blobbed, 'Net::Gearman::Blobbed::file'), 'file role');
    is( $blobbed->tmpdir, '/tmp', 'tmpdir defined');
#
    use Net::Gearman::Blobbed::memcache;
    Net::Gearman::Blobbed::memcache->mk_memcacheable( $blobbed, memcache => [] );
    isa_ok( $blobbed, 'Net::Gearman::Blobbed' );
    ok(does_role($blobbed, 'Net::Gearman::Blobbed::file'),     'file role');
    ok(does_role($blobbed, 'Net::Gearman::Blobbed::memcache'), 'file role');
    is( $blobbed->tmpdir,          '/tmp', 'tmpdir defined');
    isa_ok( $blobbed->memcache, 'Memcached::libmemcached');
}

if (DONE) { # resolution
    ok( eq_array([ Net::Gearman::Blobbed->resolve(12, 23, 34) ],
		 [ 12, 23, 34 ]), "resolve pass thru");
    throws_ok {
	Net::Gearman::Blobbed->resolve("urn:x-whatever");
    } qr/could not/, "FATAL: resolve, no roles attached";

    throws_ok {
	Net::Gearman::Blobbed->unresolve("urn:x-whatever");
    } qr/nothing/, "FATAL: unresolve, no roles attached";

    lives_ok {
    	Net::Gearman::Blobbed->delete("urn:x-whatever");
    } "WARN: delete, no roles attached";
}

done_testing;

__END__

