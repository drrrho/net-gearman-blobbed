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

#$ENV{GEARMAN} = '127.0.0.1';
unless ( $ENV{GEARMAN} ) {
    plan skip_all => 'no GEARMAN defined in ENV';
    done_testing; exit;
}

#$ENV{TMPDIR} = '/tmp/';
# unless ( $ENV{TMPDIR} ) {
#     plan skip_all => 'no TMPDIR defined in ENV';
#     done_testing; exit;
# }

#$ENV{MEMCACHE} = '127.0.0.1';
# unless ( $ENV{MEMCACHE} ) {
#     plan skip_all => 'no MEMCACHE defined in ENV';
#     done_testing; exit;
# }

#$ENV{S3HOST} = 'foo:bar@192.168.5.19:8081/mybucket';
# unless ( $ENV{S3HOST} ) {
#     plan skip_all => 'no S3HOST defined in ENV';
#     done_testing; exit;
# }

my $s3creds;
my $s3host ;
my $s3bucket;
if (defined $ENV{S3HOST}) {
    $ENV{S3HOST} =~ m{(.+?):(.+?)@(.+?)/(.+)};
    $s3creds  = { id => $1, key => $2 };
    $s3host   = $3;
    $s3bucket = $4;
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

use Net::Gearman::Blobbed::Client;
use Net::Gearman::Blobbed::Worker;
$Net::Gearman::Blobbed::log = $log;

use JSON;

if (DONE) {
    my $client = Net::Gearman::Blobbed::Client->new( gearman => $ENV{GEARMAN} );

    my @params;

    if ($ENV{S3HOST}) {
	use Net::Gearman::Blobbed::S3;
	Net::Gearman::Blobbed::S3->mk_s3able( $client, host => $s3host, credentials => $s3creds );
	push @params, "urn:x-s3:$s3bucket/aaa";
    }

    if ($ENV{MEMCACHE}) {
	use Net::Gearman::Blobbed::memcache;
	Net::Gearman::Blobbed::memcache->mk_memcacheable( $client, memcache => [ $ENV{MEMCACHE} ] );
	push @params, "urn:x-memcache:aaa";
    }

    if ($ENV{TMPDIR}) {
	use Net::Gearman::Blobbed::file;
	Net::Gearman::Blobbed::file->mk_fileable( $client, tmpdir => $ENV{TMPDIR} );
	push @params, "urn:x-file:aaa.txt";
    }


    my $pid = fork() // die "no forking ???";
    if (not $pid) {

	my $worker = Net::Gearman::Blobbed::Worker->new( gearman => $ENV{GEARMAN} );

	if ($ENV{S3HOST}) {
	    Net::Gearman::Blobbed::S3->mk_s3able( $worker, host => $s3host, credentials => $s3creds );
	}

	if ($ENV{MEMCACHE}) {
	    Net::Gearman::Blobbed::memcache->mk_memcacheable( $worker, memcache => [ $ENV{MEMCACHE} ] );
	}

	if ($ENV{TMPDIR}) {
	    Net::Gearman::Blobbed::file->mk_fileable( $worker, tmpdir => $ENV{TMPDIR} );
	}

	$worker->gearman->can_do( 'corrections' );

	while(1) {
	    $log->debug( "gearman worker waiting for job..." );

	    my $job = $worker->gearman->grab_job->get;
	    $log->info( "worker got job" );

	    use JSON qw( decode_json );
	    my @args = @{ decode_json( $job->arg )};

	    $log->info( "got job: ".join ' ', @args );
	    my @iparams = $worker->resolve( @args );

	    my $cmd = shift @args;
	    is_deeply( \@args, \@params, "worker: control urns");
	    shift @iparams;
	    is($_, 23, "worker: data") for @iparams;

	    $job->complete( encode_json( [ $worker->unresolve( $cmd, map { $_ => 24 } @args ) ] ) );
	    exit;
#	    exit if ($cmd eq 'exit');
	}
    }

    diag "waiting for worker to start ...";
    sleep 2;

    use JSON;
    my $job = $client->submit_job(
	func      => 'corrections',
	args      => [ 'whatever', map { $_ => 23 } @params ],
	on_data =>   sub {
	    $log->logdie("gearman sent partial data");
	},
	on_done =>   sub {
	    my @args = @{ decode_json( $_[0] ) };
	    is (shift @args, 'whatever', "client: first param");
	    is_deeply( \@args, \@params, "client: control urns");

	    my @oparams = $client->resolve( @args );

#warn "result ".join ' ', @oparams;
	    $client->delete( @args );
	},
	on_fail => sub {
	    my ( $msg, $name, $exception ) = @_;
	    $log->logdie( "gearman failure: $msg ".
			       ( defined $name and $name eq "gearman" and defined $exception
				     ? "$exception" : ""  ) );
	},
	);
    $job->await;
    ok(1, "jobs complete");
}

done_testing;

__END__


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
