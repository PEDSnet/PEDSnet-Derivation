#!/usr/bin/env perl
#
# $Id$

use strict;
use warnings;

use File::Temp;
use Test::More;


require_ok('PEDSnet::Derivation::Backend::CSV');

my $dir = File::Temp->newdir;

my $db = new_ok('PEDSnet::Derivation::Backend::CSV', [ db_dir => $dir->dirname ],
		'Create object');

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

undef($db);

$db = PEDSnet::Derivation::Backend::CSV->new( db_dir => $dir->dirname );
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
