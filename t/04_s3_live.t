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

#$ENV{S3HOST} = 'foo:bar@192.168.5.19:8081/mybucket';
unless ( $ENV{S3HOST} ) {
    plan skip_all => 'no S3HOST defined in ENV';
    done_testing; exit;
}
$ENV{S3HOST} =~ m{(.+?):(.+?)@(.+?)/(.+)};
my $s3creds  = { id => $1, key => $2 };
my $s3host   = $3;
my $s3bucket = $4;

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
    
    use Net::Gearman::Blobbed::S3;
    Net::Gearman::Blobbed::S3->mk_s3able( $blobbed, host => $s3host, credentials => $s3creds );

    use Moose::Util qw(does_role);
    ok(does_role($blobbed, 'Net::Gearman::Blobbed::S3'), 'S3 role');
    isa_ok($blobbed->s3, 'Net::Amazon::S3');

    $blobbed->delete("urn:x-s3:$s3bucket/aaa");

    throws_ok {
	$blobbed->resolve("urn:x-s3:$s3bucket/aaa");
    } qr{failed}, 'no key aaa yet';

    is_deeply( [ $blobbed->unresolve("urn:x-s3:$s3bucket/aaa" => encode_json([ 'something' ]) ) ],
	       [ "urn:x-s3:$s3bucket/aaa" ], 'added aaa');
    is_deeply( decode_json( ($blobbed->resolve("urn:x-s3:$s3bucket/aaa"))[0] ), [ 'something' ], 'good aaa loaded');

    is_deeply( [ $blobbed->unresolve("urn:x-s3:$s3bucket/aaa" => encode_json([ 'something else' ]),
				     "urn:x-s3:$s3bucket/bbb" => encode_json([ 'something else entirely' ]) ) ],
	       [ "urn:x-s3:$s3bucket/aaa", "urn:x-s3:$s3bucket/bbb" ], 'overruled aaa, added bbb');
    is_deeply( decode_json( ($blobbed->resolve("urn:x-s3:$s3bucket/aaa"))[0] ), [ 'something else'          ], 'good aaa loaded');
    is_deeply( decode_json( ($blobbed->resolve("urn:x-s3:$s3bucket/bbb"))[0] ), [ 'something else entirely' ], 'good bbb loaded');

    $blobbed->delete("urn:x-s3:$s3bucket/aaa");
    $blobbed->delete("urn:x-s3:$s3bucket/bbb");
}

done_testing;
