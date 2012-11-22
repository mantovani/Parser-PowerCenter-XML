#!perl -T

use Test::More tests => 1;

BEGIN {
    use_ok( 'Parser::PowerCenter::XML' ) || print "Bail out!\n";
}

diag( "Testing Parser::PowerCenter::XML $Parser::PowerCenter::XML::VERSION, Perl $], $^X" );
