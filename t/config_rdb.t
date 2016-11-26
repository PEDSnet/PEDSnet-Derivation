#!/usr/bin/env perl

use 5.010;
use strict;
use warnings;

use Test::More;

unless ( eval { require DBD::SQLite } ) {
  Test::More->import( skip_all => 'No SQLite driver' );
  exit 0;
}

### Test RDB class using in-core scratch db
package My::Test::RDB;

use 5.010;
use parent 'Rose::DB';

__PACKAGE__->use_private_registry;

__PACKAGE__->register_db( domain   => 'test',
                          type     => 'vapor',
                          driver   => 'SQLite',
                          database => ':memory:',
                        );

# SQLite in-memory db evaporates when original dbh is closed.
sub dbi_connect {
  my( $self, @args ) = @_;
  state $dbh = $self->SUPER::dbi_connect(@args);
  $dbh;
}


package main;

### Set up the test environment
my $rdb = new_ok( 'My::Test::RDB' => [ connect_options => { RaiseError => 1 },
                                       domain          => 'test',
                                       type            => 'vapor'
                                     ],
                  'Setup test db'
                );
my $dbh = $rdb->dbh;

$dbh->do('create table test (id integer primary key, name varchar(16) )');
$dbh->do(q[insert into test values (1, 'this'), (2,'that')]);


### And finally, the tests
require_ok('PEDSnet::Derivation::Config');
my $c = new_ok('PEDSnet::Derivation::Config', [ config_rdb => $rdb ]);

is_deeply( $c->ask_rdb(q[select name from test where id = 1]),
	   { name => 'this' },
	   'Simple query');

is_deeply( $c->ask_rdb(q[select * from test where id = ?], [ 2 ]),
	   { id => 2, name => 'that' },
	   'Simple query');

done_testing;
