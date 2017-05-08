#!perl


use 5.010;
use strict;
use warnings;

package PEDSnet::Derivation::Config;

our($VERSION) = '0.01';

=head1 NAME

PEDSnet::Derivation::Config - Configuration data for PEDSnet::Derivations 

=head1 SYNOPSIS

  # In derivation module
  package My::Derivation::Config;

  use Moo 2;
  use Types::Standard qw/ Bool /;
  extends 'PEDSnet::Derivation::Config';
  ...
  sub _build_config_rdb {
    my $self = shift;
    require My::RDB;
    My::RDB->new(type => $self->rdb_type, domain => $self->rdb_domain ):
  }
  ...
  has 'frobnicate' => ( isa => Bool, is => 'ro', required => 0,
       lazy => 1, builder => '_build_frobnicate' );
  sub _build_frobnicate {
    my $conf = shift;
    $conf->config_datum('frobnicate') //
    $conf->ask_rdb('select frobnicate from config_settings') // 1;
  }
  ...

  # In application
  use My::Derivation;
  my $der = My::Derivation->new(...);
  if ($der->config->frobnicate) {
    $der->add_frobnication()
  }

=head1 DESCRIPTION

L<PEDSnet::Derivation::Config> is the base class for a simple but
(hopefully) flexible way to manage configurable settings for a
derivation task.

Typically, each configuration setting you build into your derivation
subclass should be specified as an object attribute.  This helps the
user, who has a consistent way to access configuration settings.  It
also helps you (and anyone who subclasses from you), who can use
L<Moo>(se)-like tools such as types, inheritance, builders, etc. to
make configuration settings easier to use.

Values for configuration attributes may be initalized in one of
several ways:

=over 4

=item *

via a parameter in the call to the constructor

=item * 

using data from configuration file(s)

=item *

by querying a relational database

=item *

via defaults supplied in the subclass definition.

=back

There are also options for retrieving values from configuation data
that weren't defined as attributes, but that should be reserved for
uncommon cases.

While this sounds pretty complicated (and can get so), the goals are
to let you use the pieces you need and ignore the ones you don't, and
to increase the number of things your application can ignore because
a derivation-speicific configuration subclass scan supply useful defaults.

This base class provides a nahdful of common tools in service of these
goals. 

=head2 Defining sources of configuration data

=cut

use Scalar::Util qw( reftype );
use Path::Tiny qw( path );
use FindBin qw( $Bin );

use Moo 2;
use Types::Standard qw/ Maybe ArrayRef HashRef InstanceOf Str /;

=over 4

=item config_stems

=item build_config_stems

This attribute and its corresponding builder hold a reference to an
array of L<Path::Tiny> paths to potential configuration files. As the
name implies, these are handed off to L<Config::Any> if data that
might be in configuratin files is requested.

Values provided during construction are passed to L<path/Path::Tiny>,
and relative paths are converted to absolute using the location of the
running application.  This lets you pass in (possibly relative)
strings and get the right output.

If no values are provided, the default is a file with the (downcased)
name of the calling class, less any C<PEDSnet::Derivation::> prefix
and C<::Config> suffix, located in the same directory as the application.

=cut

has 'config_stems' => ( isa => ArrayRef[InstanceOf['Path::Tiny']],
			is => 'ro', required => 0,
			builder => 'build_config_stems',
			coerce => sub {
			  my $vals = shift;
			  $vals = [ $vals ] unless reftype $vals eq 'ARRAY';
			  [ map { path($_)->absolute($Bin) } @$vals ];
			});

sub build_config_stems {
  my $self = shift;
  my $class = ref($self) || $self;
  $class =~ s/^PEDSnet::Derivation:://;
  $class =~ s/::Config$//;
  $class =~ s[::][/]g;
  [ path(lc $class)->absolute($Bin) ];
}

=item config_section

=item build_config_section

This attribute/builder pair lets you specify a particular section of
the config file(s) containing the data in which you're interested.

The default is an empty string, meaning no section is used.

=cut

has 'config_section' =>
  ( isa => Maybe[Str], is => 'ro', required => 0 );

sub build_config_section { undef }

=item config_overrides

=item build_config_overrides

This hash reference (empty by default) contains key-value pairs
intended to override the content of configuration files or database
queries. 

Along with L</config_defaults>, it exists primarily to give subclasses
an efficent way to specify defaults for configuration elements that
don't require the complexity of a builder or typed attribute.

=cut

has 'config_overrides' =>
  ( isa => HashRef, is => 'ro', required => 0,
    builder => 'build_config_overrides' );

sub build_config_overrides { {} }

=item config_defaults

=item build_config_defaults

Analogously to L</config_overrides>, this hash reference (empty by
default) contains key-value pairs intended to provides defaults for
values not specified in configuration files or database queries.

=cut

has 'config_defaults' =>
  ( isa => HashRef, is => 'ro', required => 0,
    builder => 'build_config_defaults' );

sub build_config_defaults { {} }

=item config_rdb

=item build_config_rdb

This pair allows you to specify a L<Rose::DB>-compatible database
connection that can be queried to retrieve configuration data.
Changing L<config_rdb> is permitted.-

There is no default, and attempting to query the database without
specifying a value will return an empty value.

=back

=cut


has 'config_rdb' => ( isa => Maybe[InstanceOf['Rose::DB']],
		      is => 'rw', required => 0,
		      lazy => 1, builder => 'build_config_rdb' );

sub build_config_rdb { undef }

has '_config_file_content' => ( isa => HashRef, is => 'ro', required => 0,
				lazy => 1,
				builder => '_build__config_file_content' );

sub _build__config_file_content {
  my $self = shift;
  my $conf;

  require Config::Any;
  $conf =
    Config::Any->load_stems({ stems => [ map { $_->canonpath }
					 @{ $self->config_stems } ],
			      use_ext => 1,
			      driver_args =>
			      { General =>
				{ -MergeDuplicateBlocks => 1,
				  -UseApacheInclude => 1,
				  -LowerCaseNames => 1 } } });
  if ($conf and @$conf) {
    $conf = { map { %{ (%$_)[1] } } @$conf };
    my $section_key = $self->config_section;
    if ($section_key) {
      $conf = $conf->{$section_key};
    }
    return $conf;
  }
  else {
    return {};
  }
}

=head2 Retrieving configuration data

While these methods are intended primarily to let subclasses
initialize attribute values, they can also be called directly to
retrieve configuration data that may not have a full-blown attribute.

=over 4

=item config_datum($key)

Consults L</config_overrides>, the content of configuration files
specified by L</config_stems> (and L</config_section>, if any), and
finally L</config_defaults>, to find a value associated with
I<$key>. Returns the value, if found, or nothing if it is not.

=cut

sub config_datum {
  my($self, $key) = @_;
  my $store = $self->config_overrides;

  # pull out to avoid potentially unneeded and
  # expensive config file parsing 
  return $store->{$key} if exists $store->{$key};
  
  foreach $store ( $self->_config_file_content, $self->config_defaults ) {
    return $store->{$key} if exists $store->{$key};
  }
  return;
}

=item ask_rdb($sql[, $params])

Executes I<$sql> using the database connection in L</config_rdb>.  If
present, I<$params> must be a reference to an array of bind parameter
values.  When called in list context, returns the results of the query.
When called in scalar context, returns the number of results, which
is probably not what you want.

If L</config_rdb> is not defined, returns nothing,

=cut

sub ask_rdb {
  my($self, $sql, $params) = @_;
  my $rdb = $self->config_rdb;
  return unless $rdb;

  require Rose::DBx::CannedQuery;
  Rose::DBx::CannedQuery->new( rdb => $rdb, sql => $sql)->
      results(@{ $params || [] });
}

1;

__END__

=back

=head1 BUGS AND CAVEATS

Are there, for certain, but have yet to be cataloged.

=head1 VERSION

version 0.01

=head1 AUTHOR

Charles Bailey <cbail@cpan.org>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2016 by Charles Bailey

This software may be used under the terms of the Artistic License or
the GNU General Public License, as the user prefers.

This code was written at the Children's Hospital of Philadelphia as
part of L<PCORI|http://www.pcori.org>-funded work in the
L<PEDSnet|http://www.pedsnet.org> Data Coordinating Center.

=cut
