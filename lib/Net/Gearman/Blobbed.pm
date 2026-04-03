package Net::Gearman::Blobbed;

use strict;
use warnings;

use Data::Dumper;

our $VERSION = 0.01;

=pod

=head1 NAME

Net::Gearman::Blobbed - Blob support for IO::Async Gearman client and worker

=head1 SYNOPSIS

    # see Net::Gearman::Blobbed::Client
    # see Net::Gearman::Blobbed::Worker

=head1 BACKGROUND

=head2 Gearman

L<Gearman|https://gearman.org/protocol/> is a job management system where one (or more) clients can
request to a C<gearmand> server that a job - consisting of a name and an opaque parameter octet
stream - should be computed. Also to the C<gearmand> server workers can connect and register their
willingness to do such computations.

Idle workers take over such jobs, optionally report partial results or any progress and then either
complete the jobs with results or fail with errors. The C<gearmand> will pass on these results to
the requesting client.

=head2 Net::Gearman

L<Net::Gearman> is one implementation, but one based on the concepts of L<Future>s. While the
L<Net::Gearman::Worker> will simply run a loop, fetching one job after the other, the
L<Net::Gearman::Client> will submit a job which immediately results in a L<Future> object.

=head1 DESCRIPTION

This package uses L<Net::Gearman> under the hood but adds a few features:

=over

=item *

Alternatively to sending one opaque parameter to the worker (via the job server, of course),
you can use a parameter list as you would for a normal function. This parameter list will
be JSON-encoded before sending off.

=item *

As the Gearman protocol implementations pose a limit on the size of data you can send,
in this job invocation you can add an URN together with the data. That URN points to a
key/value store and to a specific location therein. Before sending the data itself, it is
written to the store and only the URN is sent via the C<gearman> protocol.

On the worker you can resolve these URNs to retrieve the actual data.

The same mechanism can be used when completed results are sent back to the client.

=back

=head2 URNs

These special URIs are used to address both, the storage backend and a specific key therein where
data should be stored or looked up. To avoid any confusion all URNs must be of the form

urn:x-I<technology>:I<key>

Examples are:

    urn:x-file:aaa.txt     # for storing in local files
    urn:x-memcache:aaa     # for Memcache
    urn:x-s3:mybucket/aaa  # for S3

These also happen to be the technologies currently bundled with L<Net::Gearman::Blobbed>. See
these subclasses on how to attach them to a worker or the client.

=head1 INTERFACE

=cut

use JSON qw(encode_json decode_json);
use Moose;

=pod

=head2 Variables

You should initialize a L<Log::Log4perl> logger and pass it into the L<Net::Gearman::Blobbed> package:

   my $log = Log::Log4perl->get_logger("whatever");

   use Net::Gearman::Blobbed;
   $Net::Gearman::Blobbed::log = $log;

Logging is doublegood.

=cut

use Log::Log4perl; # qw(:levels);
our $log;

sub DEMOLISH {}

=pod

=head2 Methods

=over

=item B<resolve>

Once you retrieved - on the worker or on the client side - a list of parameters, this method
replaces any URN references to the data with the data itself:

   my @args   = @{ decode_json( $job->arg )};        # get the list of arguments (possibly WITH URNs)
   my @params = $worker_or_client->resolve( @args ); # get the list of arguments (data, no URNs)

If a URN cannot be resolved, then an exception will be raised.

B<NOTE>: When resolving, the data will remain at the store. That is intentional, as jobs might fail
during computation and the Gearman server will task another server with the pending job. This implies
that it is your responsibilty to purge the store with C<delete> method (see below).

=cut

sub _resolve {
    my $elf = shift;
    my $urn = shift;
    $log->logdie("could not resolve '$urn'");
}

sub resolve {
    my $elf  = shift;
    my @args = @_;

    return map { /^urn:/ # TODO: others?
		     ? $elf->_resolve( $_ ) 
		     : $_ }
           @args;
}

=pod

=item B<delete>

This method takes a list of arguments (with and without URNs) and will try to delete any data to
which these URNs point to:

   $worker_or_client->delete( @args );

=cut

sub _delete {
    my $elf = shift;
    my $urn = shift;
    $log->warn("could not delete '$urn'");
}

sub delete {
    my $elf  = shift;
    my @args = @_;

    map { /^urn:/ # TODO: others?
	      ? $elf->_delete( $_ ) 
	      : $_ } # ignore these
    @args;
}

=pod

=item B<unresolve>

This method stores data under any provided URNs (yeah, probably not the most perfect name). So if
you pass on

   my @args = $client_or_worker->unresolve( "some string",
                                            23             # some number
                                            'urn:x-memcache:some-key' => $some_data->freeze );

then the frozen data will be saved into the configured B<Memcache> store. The result list contains
the original data, except those which have URNs provided. Here only the URN itself will be returned:

  @args: ( "some string", 23 'urn:x-memcache:some-key' )

=cut

sub _unresolve {
    my $elf = shift;
    my $urn  = shift;
    my $val  = shift;
    $log->logdie( "nothing is stored for $urn " );
}

sub unresolve {
    my $elf    = shift;
    my @params = @_;

    my @args;
    while (my $p = shift @params) {
	if ($p =~ m{^urn:}) { # TODO other methods?
	    my $val = shift @params;
	    push @args, $elf->_unresolve( $p, $val );

	} else {
	    push @args, $p; # as-is
	}
    }
    return @args;
}

=pod

=back

=head1 SEE ALSO

=over

=item * t/*.t test suites in this distribution

=item * L<Github|https://github.com/drrrho/net-gearman-blobbed>

=item * L<Gearman Protocol|https://gearman.org/protocol/>

=back

=head1 AUTHOR

Robert Barta, C<< <rho at devc.at> >>

=head1 LICENSE AND COPYRIGHT

Copyright 2026 Robert Barta.

This program is free software; you can redistribute it and/or modify it
under the terms of the the Artistic License (2.0). You may obtain a
copy of the full license at:

L<http://www.perlfoundation.org/artistic_license_2_0>

Any use, modification, and distribution of the Standard or Modified
Versions is governed by this Artistic License. By using, modifying or
distributing the Package, you accept this license. Do not use, modify,
or distribute the Package, if you do not accept this license.

If your Modified Version has been derived from a Modified Version made
by someone other than you, you are nevertheless required to ensure that
your Modified Version complies with the requirements of this license.

This license does not grant you the right to use any trademark, service
mark, tradename, or logo of the Copyright Holder.

This license includes the non-exclusive, worldwide, free-of-charge
patent license to make, have made, use, offer to sell, sell, import and
otherwise transfer the Package with respect to any patent claims
licensable by the Copyright Holder that are necessarily infringed by the
Package. If you institute patent litigation (including a cross-claim or
counterclaim) against any party alleging that the Package constitutes
direct or contributory patent infringement, then this Artistic License
to you shall terminate on the date that such litigation is filed.

Disclaimer of Warranty: THE PACKAGE IS PROVIDED BY THE COPYRIGHT HOLDER
AND CONTRIBUTORS "AS IS' AND WITHOUT ANY EXPRESS OR IMPLIED WARRANTIES.
THE IMPLIED WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR
PURPOSE, OR NON-INFRINGEMENT ARE DISCLAIMED TO THE EXTENT PERMITTED BY
YOUR LOCAL LAW. UNLESS REQUIRED BY LAW, NO COPYRIGHT HOLDER OR
CONTRIBUTOR WILL BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, OR
CONSEQUENTIAL DAMAGES ARISING IN ANY WAY OUT OF THE USE OF THE PACKAGE,
EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.


=cut

1;

__END__


	
	

#  	} elsif ($p =~ qr{urn:x-s3:(//(.+?)/)?(.+?)/(.+)}) {
# #	    warn "s3 $3 put";
# 	    my $bucket = $3;
# 	    my $key    = $4;

# 	    my $s3 = $elf->s3;
# 	    my $b  = $s3->bucket( $bucket );

# #	    use Time::HiRes qw ( time );
# #	    my $t0 = time();
# 	    my $val = shift @params;
# 	    $b->add_key( $key, $val, { content_type => 'application/octet-stream', content_length => length( $val ) } )
# 		or $log->logdie( $s3->err . ": " . $s3->errstr );
# #	    warn "time to save:". ( time() - $t0 );

# 	    push @args, $p;


# 	    push @args, $p;
# 	}
#     }
#     return @args;

