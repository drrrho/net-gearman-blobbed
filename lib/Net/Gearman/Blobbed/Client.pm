package Net::Gearman::Blobbed::Client;

use strict;
use warnings;

use Data::Dumper;

=pod

=head1 NAME

Net::Gearman::Blobbed::Client - Gearman client with large blob support

=head1 SYNOPSIS

    use Net::Gearman::Blobbed::Client;
    $Net::Gearman::Blobbed::log = $TM2::log;
    my $client = Net::Gearman::Blobbed::Client->new(
	gearman => '10.10.58.127',
	);

    use Net::Gearman::Blobbed::memcache;
    Net::Gearman::Blobbed::memcache->mk_memcacheable( $client, memcache => [ '10.10.58.127' ] );

    use Net::Gearman::Blobbed::S3;
    Net::Gearman::Blobbed::S3->mk_s3able( $client, host => '10.10.58.127', credentials => { id => 'foo', key => 'bar' } );

    use JSON;
    my $job = $client->submit_job(
	func      => 'som-important-computation',
	args      => [ 'whatever', 'urn:x-memcache:arg1' => $huge_bin_data ],
	on_data =>   sub {
	    warn "gearman sent partial data";
	},
	on_done =>   sub {
	    my @args = @{ decode_json( $_[0] ) };
	    my @oparams = $client->resolve( @args );
            warn "have results: ".Dumper \@oparams;
	    $client->delete( @args );
	},
	on_fail => sub {
	    my ( $msg, $name, $exception ) = @_;
	    die "gearman failure: $msg ".
			       ( defined $name and $name eq "gearman" and defined $exception
				     ? "$exception" : ""  );
	},
	);
    $job->await;

=head1 INTERFACE

=head2 Constructor

This class represents the behaviour of a Gearman client. It inherits the resolution mechanism from
L<Net::Gearman::Blobbed>. When instantiating, you provide the address of the Gearman server; the
necessary client infrastructure will be built from that.

=cut

use Moose;
extends 'Net::Gearman::Blobbed';

=pod

=head2 Attributes

=over

=item B<gearman>

Read-only accessor to retrieve the original L<Net::Gearman::Client>.

=cut

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

=pod

=back

=head2 Methods

=over

=item B<submit_job>

This method will return a L<Future> object. You will have to I<await> these futures at some point.

As input you pass in a hash (not a reference) with the following fields:

=over

=item * C<func>

The name of the function on the worker.

=item * C<unique_id>

Optionally, you can pass in your own unique ID which will be used for this invocation. See
the protocol specification.

=item * C<arg>

If provided, this binary data (it will be treated as such) will be directly passed as-is on to the Gearman server.

=item * C<args>

If provided, this list reference should contain any arguments to be sent to the Gearman server. This list can
contain URNs, in which case any associated data will be stored to the indicated backend.

=item * C<on_data>

This sub is directly passed on to the B<gearman> attribute.

=item * C<on_status>

This sub is directly passed on to the B<gearman> attribute.

=item * C<on_done>

This sub is attached to the newly created job.

=item * C<on_fail>

This sub is attached to the newly created job.

=back

=cut

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

=pod

=back

=head1 AUTHOR

Robert Barta, C<< <rho at devc.at> >>

=head1 LICENSE AND COPYRIGHT

Copyright 2026 Robert Barta.

=cut

1;
