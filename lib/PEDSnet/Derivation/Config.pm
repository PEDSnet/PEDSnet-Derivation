#!perl
#
# $Id$

use 5.010;
use strict;
use warnings;

package PEDSnet::Derivation::Config;

our($VERSION) = '0.01';
our($REVISION) = '$Revision$' =~ /: (\d+)/ ? $1 : 0;

use Scalar::Util qw( reftype );
use Path::Tiny qw( path );
use FindBin qw( $Bin );

use Moo 2;
use Types::Standard qw/ ArrayRef HashRef InstanceOf /;

has '_config_stems' => ( isa => ArrayRef[InstanceOf['Path::Tiny']],
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

has '_config_file_content' => ( isa => HashRef, is => 'ro', required => 0,
				lazy => 1,
				builder => '_build__config_file_content' );

sub _build__config_file_content {
  my $self = shift;
  my $conf;

  require Config::Any;
  $conf =
    Config::Any->load_stems({ stems => [ map { $_->canonpath } @{ $self->_config_stems } ],
			      use_ext => 1,
			      driver_args =>
			      { General =>
				{ -MergeDuplicateBlocks => 1,
				  -LowerCaseNames => 1 } } });
  if ($conf and @$conf) {
    return { map { %{ (%$_)[1] } } @$conf };
  }
  else {
    return {};
  }
}

has 'config_rdb' => ( isa => InstanceOf['Rose::DB'], is => 'rw', required => 0 );

sub ask_rdb {
  my($self, $sql, $params) = @_;
  my $rdb = $self->config_rdb;
  return unless $rdb;

  require Rose::DBx::CannedQuery;
  Rose::DBx::CannedQuery->new( rdb => $rdb, sql => $sql)->
      results(@{ $params || [] });
}

1;
