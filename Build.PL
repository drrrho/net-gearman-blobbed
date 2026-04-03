use 5.006;
use strict;
use warnings;
use Module::Build;
Module::Build->VERSION('0.4004');

# $ENV{'PERL_MB_OPT'} = '--install_path lib=/usr/share/perl5';
$ENV{'DEB_BUILD_OPTIONS'}='nocheck';

my $debian = Module::Build->subclass(
    class => "Module::Build::Debian",
    code => <<'SUBCLASS' );

sub DEBVERSION {
    return 2;
}

sub ACTION_README {
    my $self = shift;

    $self->do_system(qq{   pod2readme -f markdown lib/Net/Gearman/Blobbed.pm } );
}

sub ACTION_cpan_upload {
    my $self = shift;

    $self->depends_on("distdir");
    $self->depends_on("dist");
    my $version = $self->dist_version;
    $self->do_system(qq{   cpan-upload Net-Gearman-Blobbed-${version}.tar.gz } );
}

sub ACTION_debian {
    my $self = shift;

    $self->depends_on("README");
    $self->depends_on("dist");
    $self->depends_on("distdir");

    my $version = $self->dist_version;
    my $debversion = $self->DEBVERSION;
    $self->do_system(qq{ DEB_BUILD_OPTIONS=nocheck  dh-make-perl --build --version=$version-$debversion Net-Gearman-Blobbed-$version } );
}

sub ACTION_inject {
    my $self = shift;

    $self->depends_on("debian");

    my $version = $self->dist_version;
    my $debversion = $self->DEBVERSION;
    $self->do_system(qq{ scp libnet-gearman-blobbed-perl_${version}-${debversion}_all.deb packages.devc.at:packages/ } );
    foreach (qw(stretch buster bullseye) ) { # squeeze wheezy jessie 
        $self->do_system(qq{ ssh packages.devc.at -Y -t reprepro --ask-passphrase -b /var/www/packages/data/dists/ includedeb $_ packages/libnet-gearman-blobbed-perl_${version}-${debversion}_all.deb });
    }
}
SUBCLASS

my $builder = $debian->new(
    module_name         => 'Net::Gearman::Blobbed',
    license             => 'artistic_2',
    dist_author         => q{Robert Barta <rho@devc.at>},
    dist_version_from   => 'lib/Net/Gearman/Blobbed.pm',
    dist_abstract       => 'Wrapper around IO::Async Gearman client/worker to support large data',
    release_status      => 'stable',
#   install_path        => {lib  => '/usr/share/perl5', },
    configure_requires => {
        'Module::Build' => '0.4004',
    },
    test_requires => {
        'Test::More' => undef,
	    'Test::Exception'     => undef,
	    'JSON'                => undef,
	    'Moose::Util'         => undef,
	    'Pod::Coverage'       => undef,
	    'Test::CheckManifest' => undef,
	    'Test::Deep'          => undef,
	    'Test::Pod'           => undef,
	    'Log::Log4perl'       => undef,
    },
    requires => {
	'Memcached::libmemcached'  => undef,
	    'Net::Amazon::S3'      => undef,
	    'IO::File'             => undef,
	    'Moose'                => undef,
	    'Moose::Util'          => undef,
	    'Log::Log4perl'        => undef,
	    'JSON'                 => undef,
	    'Time::HiRes'          => undef,
	    'Net::Gearman::Client' => undef,
	    'Net::Gearman::Worker' => undef,
    },
    add_to_cleanup     => [ 'Net-Gearman-Blobbed-*' ],
);

$builder->create_build_script();

__END__

# grep use t/*.t |egrep -v '(strict|warning)' | perl -ne '/use\s+(.+?);/; print "$1\n"' |sort -u

