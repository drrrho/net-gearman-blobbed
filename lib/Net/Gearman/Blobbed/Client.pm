package Net::Gearman::Blobbed::Client;

use strict;
use warnings;

use Data::Dumper;

use Moose;
extends 'Net::Gearman::Blobbed';

has 'gearman' => (
    is => 'ro',
    isa => 'Net::Gearman::Client',
    );

around BUILDARGS => sub {
    my $orig  = shift;
    my $class = shift;
    my %options = @_;

    use Net::Gearman::Client;
    my $client = Net::Gearman::Client->new(
	PeerAddr => delete $options{gearman},
	) or $Net::Gearman::Blobbed::log->logdie( "Cannot connect to gearmand: $@" );
    $client->option_request( "exceptions" )->get;

    return $class->$orig (gearman  => $client, %options);
};

sub submit_job {
    my $elf = shift;
    my $client = $elf->gearman;
    my %options = @_;

    my $arg;
    if ($arg = $options{arg}) {
	# happy camper

    } elsif (my @args = @{ $options{args} }) {
	my @unresolved = $elf->unresolve( @args );
	use JSON;
	$arg = encode_json( \@unresolved );

    } else {
	$Net::Gearman::Blobbed::log->warn( "no args?" );
	$arg = encode_json( [] );
    }

    my $job = $client->submit_job(
	func      => $options{func},
	unique_id => $options{unique_id},
	arg       => $arg,
	on_data   => $options{on_data},
	on_status => $options{on_status},
	);
    $job->on_done( $options{on_done} );
    $job->on_fail( $options{on_fail} );
    return $job;
}

1;
