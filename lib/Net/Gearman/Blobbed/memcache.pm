package Net::Gearman::Blobbed::memcache;

use strict;
use warnings;

use Moose::Role;

use Net::Gearman::Blobbed;
use Memcached::libmemcached qw(memcached_quit memcached_create memcached_server_add memcached_set memcached_get memcached_delete);

has 'memcache' => (
    is => 'ro',
    isa => 'Memcached::libmemcached',
    );

sub mk_memcacheable {
    shift;
    my $elf = shift;
    my %params = @_;

    my $memc = memcached_create();
    $Net::Gearman::Blobbed::log->debug( "creating memcache client: ".$memc->errstr() );

    map {
	memcached_server_add($memc, $_ );
	$Net::Gearman::Blobbed::log->debug( "connecting to memcache $_: ".$memc->errstr() );
    } @{ delete $params{memcache} // [] };

    use Moose::Util qw(apply_all_roles);
    apply_all_roles($elf, 'Net::Gearman::Blobbed::memcache' => {
	                                                         rebless_params => {
								     memcache => $memc
								 } });
}

before 'DEMOLISH' => sub {
    my $elf = shift;
    my $memc = $elf->memcache;
    memcached_quit( $memc );
};

around '_resolve' => sub {
    my $orig = shift;
    my $elf  = shift;
    my $urn  = shift;

    if ($urn =~ qr{^urn:x-memcache:(//(.+?)/)?(.+)}) {
	my $memc = $elf->memcache;
	if ($2) {
	    memcached_server_add($memc, $2 );
	    $Net::Gearman::Blobbed::log->debug( "added adhoc memcache $2: ".$memc->errstr() );
	}
	my $key = $3;
#warn "resolve memcache $key";
	my $val = memcached_get($memc, $key);
	$Net::Gearman::Blobbed::log->debug( "got key '$key': ".$memc->errstr() );
#	memcached_delete($memc, $key);
#	$Net::Gearman::Blobbed::log->debug( "deleted key '$key': ".$memc->errstr() );
	return $val;

    } else {
	return $elf->$orig($urn, @_); # maybe something else? 
    }
};

around '_delete' => sub {
    my $orig = shift;
    my $elf  = shift;
    my $urn  = shift;

    if ($urn =~ qr{^urn:x-memcache:(//(.+?)/)?(.+)}) {
	my $memc = $elf->memcache;
	if ($2) {
	    memcached_server_add($memc, $2 );
	    $Net::Gearman::Blobbed::log->debug( "added adhoc memcache $2: ".$memc->errstr() );
	}
	my $key = $3;
#warn "resolve memcache $key";
#	my $val = memcached_get($memc, $key);
#	$Net::Gearman::Blobbed::log->debug( "got key '$key': ".$memc->errstr() );
	memcached_delete($memc, $key);
	$Net::Gearman::Blobbed::log->debug( "deleted key '$key': ".$memc->errstr() );
#	return $val;

    } else {
	$elf->$orig($urn, @_); # maybe something else? 
    }
};

around '_unresolve' => sub {
    my $orig = shift;
    my $elf  = shift;
    my $urn  = shift;
    my $val  = shift;

    if ($urn =~ qr{urn:x-memcache:(//(.+?)/)?(.+)}) {
	my $memc = $elf->memcache;
	if ($2) {
	    memcached_server_add($memc, $2 );
	}
	my $key = $3;
#warn "_unresolve $key";
	memcached_set($memc, $key, $val);
	$Net::Gearman::Blobbed::log->debug( "set key '$key': ".$memc->errstr() );
	return $urn;

    } else {
	return $elf->$orig($urn, $val, @_)
    }
};

1;


__END__

          sub make_breakable {
               my ( $self, %params ) = @_;
               apply_all_roles($self, 'Breakable', { rebless_params => \%params });
           }

           my $car = Car->new();
           $car->make_breakable( breakable_parts => [qw( tires wheels windscreen )] );
