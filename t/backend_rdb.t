#!/usr/bin/env perl
#
# $Id$

use strict;
use warnings;

use Test::More;


require_ok('PEDSnet::Derivation::Backend::RDB');

unless ( eval { require DBD::SQLite } ) {
  done_testing();
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

my $rdb =
  PEDSnet::Derivation::Backend::RDB::_dsn_to_rdb('dbi:SQLite:database=:memory:');

isa_ok($rdb, 'Rose::DB');

$rdb = PEDSnet::Derivation::Backend::RDB::_dsn_to_rdb({ type => 'vapor',
							domain => 'test',
							driver => 'SQLite',
							database => ':memory:'} );

isa_ok($rdb, 'Rose::DB');


my $db = new_ok('PEDSnet::Derivation::Backend::RDB',
		[ rdb => My::Test::RDB->new( domain => 'test', type => 'vapor') ],
		'Create backend object');

ok($db->build_query('create table testme (id INT, label VARCHAR(20))')->execute,
   'Create base table');

my $qry = $db->get_query('insert into testme (id, label) VALUES (?,?), (?,?)');
ok($qry, 'Insert statement');

is($db->store_chunk($qry, [ { id => 1, label => 'a', junk => 'stuff' },
			    { id => 2, label => 'b', junk => 'stuff' }]),
   2, 'Insert rows');

ok($db->clone_table('testme', 'testme2'), 'Clone table');

is_deeply( [ sort $db->column_names('testme') ],
	   [ qw/ id label / ], 'Column names');

$qry = $db->get_query('select * from testme where id < ? order by id');
ok($db->execute($qry, [ 10 ]), 'Execute fetch');

is_deeply($db->fetch_chunk($qry,1),
	  [ { id => 1, label => 'a' } ], 'Got first row');

is_deeply($db->fetch_chunk($qry,1),
	  [ { id => 2, label => 'b' } ], 'Got second row');

$db->execute($qry, [ 10 ]);
is_deeply($db->fetch_chunk($qry),
	  [ { id => 1, label => 'a' },
	    { id => 2, label => 'b' } ], 'Got full set');
is_deeply($db->fetch_chunk($qry), [ ],
   'Returns no data when query not active');

Test::More::done_testing();
