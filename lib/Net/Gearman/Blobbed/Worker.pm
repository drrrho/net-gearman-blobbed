package Net::Gearman::Blobbed::Worker;

use strict;
use warnings;

use Data::Dumper;

=pod

=head1 NAME

Net::Gearman::Blobbed::Worker - Gearman worker with large blob support

=head1 SYNOPSIS

  use Net::Gearman::Blobbed::Worker;
  my $worker = Net::Gearman::Blobbed::Worker->new( gearman => '10.10.58.127' );

  use Net::Gearman::Blobbed::S3; # as example
  Net::Gearman::Blobbed::S3->mk_s3able( $worker, host => $s3host, credentials => $s3creds );

  while(1) {
     my $job = $worker->gearman->grab_job->get;

     use JSON qw( decode_json );
     my @args = @{ decode_json( $job->arg )};

     my @iparams = $worker->resolve( @args );

     # do something

     $job->complete( encode_json( [ $worker->unresolve( ... results ... ) ] ) );
     $worker->delete( @args ); # do not forget to clean up
  }

=head1 INTERFACE

=head2 Constructor

This class represents the behaviour of a Gearman worker. It inherits the resolution mechanism from
L<Net::Gearman::Blobbed>. When instantiating, you provide the address of the Gearman server; the
necessary worker infrastructure will be built from that.

=head2 Attributes

=over

=item B<gearman>

Read-only accessor to retrieve the original L<Net::Gearman::Worker>.

=back

=cut

use Moose;
extends 'Net::Gearman::Blobbed';

has 'gearman' => (
    is => 'ro',
    isa => 'Net::Gearman::Worker',
    );

around BUILDARGS => sub {
    my $orig  = shift;
    my $class = shift;
    my %options = @_;

    use Net::Gearman::Worker;
    my $worker = Net::Gearman::Worker->new(
	PeerAddr => delete $options{gearman},
	) or $Net::Gearman::Blobbed::log->logdie( "Cannot connect to gearmand: $@" );

    return $class->$orig (gearman  => $worker, %options);
};

=pod

=head1 AUTHOR

Robert Barta, C<< <rho at devc.at> >>

=head1 LICENSE AND COPYRIGHT

Copyright 2026 Robert Barta.

=cut

1;
