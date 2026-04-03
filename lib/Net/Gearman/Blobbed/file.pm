package Net::Gearman::Blobbed::file;

use strict;
use warnings;

use Data::Dumper;

use Moose::Role;

use Net::Gearman::Blobbed;

has 'tmpdir' => (
    is  => 'rw',
    isa => 'Str',
    );

sub mk_fileable {
    shift;
    my $elf = shift;
    my %options = @_;

    use Moose::Util qw(apply_all_roles);
    apply_all_roles($elf, 'Net::Gearman::Blobbed::file' => {
	rebless_params => {
	    tmpdir => $options{tmpdir}
	} });
}

before 'DEMOLISH' => sub {
    my $elf = shift;
};

use IO::File;
around '_resolve' => sub {
    my $orig = shift;
    my $elf  = shift;
    my $urn  = shift;

    if ($urn =~ qr{^urn:x-file:(.+)}) {
	my $file = $elf->tmpdir . $1;
	my $fh = IO::File->new();
	my $val;
	if ($fh->open("< $file")) {
	    $fh->binmode;
	    $val = <$fh>;
	    $fh->close;
	    $Net::Gearman::Blobbed::log->debug( "file key '$file' read" );
	} else {
	    $Net::Gearman::Blobbed::log->logdie( "file key '$file': open readonly failed $!" );
	}
	return $val;

    } else {
	return $elf->$orig($urn, @_); # maybe something else? 
    }
};

around '_delete' => sub {
    my $orig = shift;
    my $elf  = shift;
    my $urn  = shift;

    if ($urn =~ qr{^urn:x-file:(.+)}) {
	my $file = $elf->tmpdir . $1;
	unlink $file;
	$Net::Gearman::Blobbed::log->debug( "file key '$file' deleted" );

    } else {
	$elf->$orig($urn, @_); # maybe something else? 
    }
};

around '_unresolve' => sub {
    my $orig = shift;
    my $elf  = shift;
    my $urn  = shift;
    my $val  = shift;

    if ($urn =~ qr{urn:x-file:(.+)}) {
	my $file = $elf->tmpdir . $1;
	my $fh = IO::File->new("> $file");
	if (defined $fh) {
	    $fh->binmode;
	    print $fh $val;
	    $fh->close;
	    $Net::Gearman::Blobbed::log->debug( "file key '$file' written" );
	} else {
	    $Net::Gearman::Blobbed::log->logdie( "file key '$file': open write failed $!" );
	}
	return $urn;

    } else {
	return $elf->$orig($urn, $val, @_)
    }
};

1;


__END__
