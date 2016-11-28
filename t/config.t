#!/usr/bin/env perl
#
# $Id$

use strict;
use warnings;

use Test::More;
use FindBin;
use Path::Tiny;

require_ok('PEDSnet::Derivation::Config');

is(PEDSnet::Derivation::Config->
   new(config_stems => [ 'foo' ])->config_stems->[0]->canonpath,
   path('foo')->absolute($FindBin::Bin)->canonpath,
   'Specified relative config_stem'
  );

is(PEDSnet::Derivation::Config->
   new(config_stems => [ '/foo/bar' ])->config_stems->[0]->canonpath,
   path('/foo/bar')->absolute($FindBin::Bin)->canonpath,
   'Specified absolute config_stem'
  );

{
  package PEDSnet::Derivation::Foo::Bar::Config;
  our(@ISA) = ('PEDSnet::Derivation::Config');

  Test::More::is(PEDSnet::Derivation::Foo::Bar::Config->new->
		 config_stems->[0]->canonpath,
		 Path::Tiny::path('foo/bar')->
		 absolute($FindBin::Bin)->canonpath,
		 'Default config_stem'
		);
}

is_deeply(PEDSnet::Derivation::Config->new->_config_file_content,
	  { test => 'me',
	    cause => 'effect',
	    hash => { key1 => 'value1', key2 => 'value2' }},
	  'Config file contents');

my $conf = new_ok('PEDSnet::Derivation::Config',
		  [ config_overrides => { test => 'you' },
		    config_defaults =>  { latent => 'attribute' } ]);
is($conf->config_datum('test'), 'you', 'Config override');
is($conf->config_datum('cause'), 'effect', 'Config file');
is($conf->config_datum('latent'), 'attribute', 'Config default');

done_testing;
