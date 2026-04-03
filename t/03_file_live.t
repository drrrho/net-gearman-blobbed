use strict;
use warnings;

use Test::More;
use Test::Exception;
use Test::Deep;

use Data::Dumper;
$Data::Dumper::Indent = 1;

#$ENV{TMPDIR} = '/tmp/';
unless ( $ENV{TMPDIR} ) {
    plan skip_all => 'no TMPDIR defined in ENV';
    done_testing; exit;
}

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

use JSON;

if (DONE) {
    my $blobbed = Net::Gearman::Blobbed->new;

    use Net::Gearman::Blobbed::file;
    Net::Gearman::Blobbed::file->mk_fileable( $blobbed, tmpdir => $ENV{TMPDIR} );

    lives_ok {
	$blobbed->delete('urn:x-file:aaa.txt');
	$blobbed->delete('urn:x-file:bbb.txt');
    } 'deleting non-existing file';
    throws_ok {
	$blobbed->resolve('urn:x-file:aaa.txt');
    } qr/No such file/, 'try to resolve non-existing file';
#
    is_deeply( [ $blobbed->unresolve('urn:x-file:aaa.txt' => encode_json([ 'something' ]) ) ],
	       [ 'urn:x-file:aaa.txt' ], 'added aaa');
    is_deeply( decode_json( ($blobbed->resolve('urn:x-file:aaa.txt'))[0] ), [ 'something' ], 'good aaa loaded');

    is_deeply( [ $blobbed->unresolve('urn:x-file:aaa.txt' => encode_json([ 'something else' ]),
    				     'urn:x-file:bbb.txt' => encode_json([ 'something else entirely' ]) ) ],
    	       [ 'urn:x-file:aaa.txt', 'urn:x-file:bbb.txt' ], 'overruled aaa, added bbb');

    is_deeply( decode_json( ($blobbed->resolve('urn:x-file:aaa.txt'))[0] ), [ 'something else'          ], 'good aaa loaded');
    is_deeply( decode_json( ($blobbed->resolve('urn:x-file:bbb.txt'))[0] ), [ 'something else entirely' ], 'good bbb loaded');

    $blobbed->delete('urn:x-file:aaa.txt');
    $blobbed->delete('urn:x-file:bbb.txt');
}

done_testing;

__END__
