#!perl
use 5.006;
use strict;
use warnings;
use Test::More;

plan tests => 1;

BEGIN {
    use_ok( 'Net::Gearman::Blobbed' ) || print "Bail out!\n";
}

diag( "Testing Net::Gearman::Blobbed $Net::Gearman::Blobbed::VERSION, Perl $], $^X" );
