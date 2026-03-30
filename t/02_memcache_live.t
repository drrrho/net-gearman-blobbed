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

#$ENV{MEMCACHE} = '127.0.0.1';
unless ( $ENV{MEMCACHE} ) {
    plan skip_all => 'no MEMCACHE defined in ENV';
    done_testing; exit;
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

use JSON;

if (DONE) {
    my $blobbed = Net::Gearman::Blobbed->new;
    
    use Net::Gearman::Blobbed::memcache;
    Net::Gearman::Blobbed::memcache->mk_memcacheable( $blobbed, memcache => [ $ENV{MEMCACHE} ] );
    use Moose::Util qw(does_role);
    ok(does_role($blobbed, 'Net::Gearman::Blobbed::memcache'), 'memcache role');
    isa_ok($blobbed->memcache, 'Memcached::libmemcached');

    $blobbed->delete('urn:x-memcache:aaa');
    is_deeply( [ $blobbed->resolve('urn:x-memcache:aaa')], [undef], 'no aaa yet');
    is_deeply( [ $blobbed->unresolve('urn:x-memcache:aaa' => encode_json([ 'something' ]) ) ],
	       [ 'urn:x-memcache:aaa' ], 'added aaa');
    is_deeply( decode_json( ($blobbed->resolve('urn:x-memcache:aaa'))[0] ), [ 'something' ], 'good aaa loaded');

    is_deeply( [ $blobbed->unresolve('urn:x-memcache:aaa' => encode_json([ 'something else' ]),
				     'urn:x-memcache:bbb' => encode_json([ 'something else entirely' ]) ) ],
	       [ 'urn:x-memcache:aaa', 'urn:x-memcache:bbb' ], 'overruled aaa, added bbb');
    is_deeply( decode_json( ($blobbed->resolve('urn:x-memcache:aaa'))[0] ), [ 'something else'          ], 'good aaa loaded');
    is_deeply( decode_json( ($blobbed->resolve('urn:x-memcache:bbb'))[0] ), [ 'something else entirely' ], 'good bbb loaded');

    $blobbed->delete('urn:x-memcache:aaa');
    $blobbed->delete('urn:x-memcache:bbb');
}

done_testing;
