package Net::Gearman::Blobbed::S3;

use strict;
use warnings;

use Moose::Role;

use Net::Gearman::Blobbed;

has 's3' => (
    is => 'ro',
    isa => 'Net::Amazon::S3',
    );

sub mk_s3able {
    shift;
    my $elf = shift;
    my %params = @_;

    use Net::Amazon::S3;
    use Net::Amazon::S3::Vendor::Generic;
    my $s3 = Net::Amazon::S3->new (
	vendor => Net::Amazon::S3::Vendor::Generic->new (
	    host => $params{host}, # '192.168.5.19:8081',
	    use_https => 0,
	),
	aws_access_key_id     => $params{credentials}->{id}, # 'foo',
	aws_secret_access_key => $params{credentials}->{key}, # 'bar',
	);
    $Net::Gearman::Blobbed::log->debug( "connect to S3" );

    use Moose::Util qw(apply_all_roles);
    apply_all_roles($elf, 'Net::Gearman::Blobbed::S3' => {
	rebless_params => {
	    s3 => $s3
	} });
}

before 'DEMOLISH' => sub {};

around '_unresolve' => sub {
    my $orig = shift;
    my $elf  = shift;
    my $urn  = shift;
    my $val  = shift;

    if ($urn =~ q{urn:x-s3:(//(.+?)/)?(.+?)/(.+)}) {
#warn "s3 $3 put";
	my $bucket = $3;
	my $key    = $4;
#warn "bucket $bucket key $key";
	my $s3 = $elf->s3;
	my $b  = $s3->bucket( $bucket );

	$b->add_key( $key, $val, { content_type => 'application/octet-stream', content_length => length( $val ) } )
	    or $Net::Gearman::Blobbed::log->logdie( $s3->err . ": " . $s3->errstr );
	$Net::Gearman::Blobbed::log->debug( "in bucket $bucket set key '$key'" );
	return $urn;

    } else {
	return $elf->$orig($urn, $val, @_);
    }
};

around '_resolve' => sub {
    my $orig = shift;
    my $elf  = shift;
    my $urn  = shift;

    if ($urn =~ q{urn:x-s3:(//(.+?)/)?(.+?)/(.+)}) {
	my $bucket = $3;
	my $key    = $4;
	my $s3 = $elf->s3;
	my $b = $s3->bucket( $bucket );

	my $response = $b->get_key( $key )
	    or $Net::Gearman::Blobbed::log->logdie( $s3->err . ": " . $s3->errstr );
	$Net::Gearman::Blobbed::log->debug( "in bucket $bucket got key '$key'" );
	return $response->{value};

    } else {
	return $elf->$orig($urn, @_);
    }
};

around '_delete' => sub {
    my $orig = shift;
    my $elf  = shift;
    my $urn  = shift;

    if ($urn =~ q{urn:x-s3:(//(.+?)/)?(.+?)/(.+)}) {
	my $bucket = $3;
	my $key    = $4;
	my $s3 = $elf->s3;
	my $b = $s3->bucket( $bucket );

	my $response = $b->delete_key( $key )
	    or $Net::Gearman::Blobbed::log->logdie( $s3->err . ": " . $s3->errstr );
	$Net::Gearman::Blobbed::log->debug( "in bucket $bucket delete key '$key'" );

    } else {
	$elf->$orig($urn, @_);
    }
};

1;

__END__
