package Net::Gearman::Blobbed;

use strict;
use warnings;

use Data::Dumper;

use JSON qw(encode_json decode_json);
use Moose;

# use TM2; # for log, to be removed

use Log::Log4perl; # qw(:levels);
our $log;

around BUILDARGS => sub {
    my $orig  = shift;
    my $class = shift;
    my %options = @_;

    return $class->$orig (%options);
};

sub DEMOLISH {}

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

1;
