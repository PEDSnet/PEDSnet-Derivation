#!/usr/bin/env perl

use Test::More;

require_ok('PEDSnet::Derivation');

require PEDSnet::Derivation::Backend::CSV;

my $end = PEDSnet::Derivation::Backend::CSV->new(db_dir => '.');
my $d = new_ok('PEDSnet::Derivation' => [ src_backend => $end,
					  sink_backend => $end ]);

isa_ok($d->config, 'PEDSnet::Derivation::Config');

done_testing;

