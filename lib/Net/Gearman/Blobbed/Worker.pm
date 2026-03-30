package Net::Gearman::Blobbed::Worker;

use strict;
use warnings;

use Data::Dumper;

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

1;
