#!perl
#
# $Id$

use 5.010;
use strict;
use warnings;

package PEDSnet::Derivation::Backend::RDB;

our($VERSION) = '0.01';
our($REVISION) = '$Revision$' =~ /: (\d+)/ ? $1 : 0;

use Scalar::Util qw/ blessed reftype /;

use Moo 2;
use Types::Standard qw/ InstanceOf HashRef /;

use Rose::DBx::CannedQuery::Glycosylated;

extends 'PEDSnet::Derivation::Backend';
with 'MooX::Role::Chatty';

has 'rdb' => ( isa => InstanceOf [ 'Rose::DB' ],
	       is => 'ro', required => 1, lazy => 1,
	       coerce => sub { blessed $_[0] ? $_[0] : _dsn_to_rdb(@_) },
               builder => 'build_rdb' );

sub _build_rdb {
  die __PACKAGE__ . "requires 'rdb' attribute\n";
}

sub _dsn_to_rdb {
  my $dsn = shift;

  if (ref $dsn) {
    if (reftype $dsn eq 'HASH') {
      state $counter = 0;
      my $pack = __PACKAGE__ . sprintf('::RDB::GEN%03d', $counter++);
      eval qq[{ package $pack; ] .
	q[use parent 'Rose::DBx::MoreConfig'; ] .
	q[__PACKAGE__->use_private_registry; ] .
	q[__PACKAGE__->register_db(] .
	join(',', map { "'$_' => '$dsn->{$_}'" } keys %$dsn) .
	q[); __PACKAGE__->auto_load_fixups; 1; }];
      $pack->new( map { $_ => $dsn->{$_} } qw/ type domain /);
    }
  }
  elsif ($dsn =~ /^dbi:(\w+)/) {
    my $db = Rose::DB->new(driver => $1);
    my $schema;
    if ($dsn =~ /schema=(\w+)/) {
      $schema = $1;
      $dsn =~ s/;?schema=$schema//;
    }
    $db->dsn($dsn);
    $db->post_connect_sql("set search_path to $schema")
      if $schema and $db->driver eq 'Pg';
    return $db;
  }
  else {
    my($type, @params) = split /;/, $dsn;
    require Module::Runtime;
    Module::Runtime::require_module($type);
    $type->new(@params);
  }
}

has '_qry_info' =>
  ( isa => HashRef, is => 'ro', default => sub { {} }, init_arg => undef );

sub column_names {
  my($self,$table) = @_;
  my $rdb = $self->rdb;
  my $sth = $rdb->dbh->column_info(undef,
				   $rdb->database,
				   $rdb->schema,
				   $table);
  my $rslt = $sth->fetchall_arrayref({});
  my(@cols);
  
  # Some databases (SQLite, looking at you) don't implement column_info
  if (@$rslt) {
    @cols = map { $_->{COLUMN_NAME} } @$rslt;
  }
  else {
    $sth = $self->build_query('select * from ' .
			      $rdb->dbh->quote_identifier($table) .
			      ' limit 1' )->execute;
    @cols = @{ $sth->{NAME} };
  }
  $sth->finish;
  @cols;
}

sub clone_table {
  my($self,$src,$dest) = @_;
  $self->remark({ level => 2,
		  message => "Cloning structure of $src to $dest" });
  my $dbh = $self->rdb->dbh;
  my($src_id, $dest_id);

  foreach my $pair ( [ $src => \$src_id ], [ $dest => \$dest_id ]) {
    if ($pair->[0] =~ /\./) {
      ${ $pair->[1] } =
	join '.',
	map { $dbh->quote_identifier($_) }
	split /\./, $pair->[0];
    }
    else {
      ${ $pair->[1] } = $dbh->quote_identifier($pair->[0]);
    }
  }

  if ($self->rdb->driver eq 'Pg') {
    return unless defined
      $dbh->do("CREATE TABLE $dest_id LIKE $src_id INCLUDING ALL");
  }
  else {
    if (not defined eval { local $SIG{__WARN__} = sub {};
			   $dbh->do("CREATE TABLE $dest_id LIKE $src_id") }) {
      return unless defined 
	 $dbh->do("CREATE TABLE $dest_id AS SELECT * FROM $src_id WHERE 1=0");
    }
  }
  
  return $dest;
}


sub _can_it {
  my($self, $method, $sql) = @_;
  Rose::DBx::CannedQuery::Glycosylated->$method(rdb => $self->rdb,
						sql => $sql,
						verbose => $self->verbose,
						logger => $self->logger);
}

sub build_query { shift->_can_it('new', @_);}
sub get_query   { shift->_can_it('new_or_cached', @_);}

sub execute {
  my($self,$qry,$params) = @_;
  $params //= [];
  $self->remark({ level => 2,
		  message => 'Executing' .
		  (@$params ? ' with ' . join(', ', @$params) : '') });
  my $rv = $qry->execute(@$params);
  unless ($rv) {
    $self->logger->warn('Query failed with error: ' . DBI->errstr);
    return;
  }
  $qry->sth;
}
  
sub fetch_chunk {
  my($self, $qry, $count) = @_;
  my $sth = $qry->sth;
  return [] unless $sth and $sth->{Active};
  my $rows = $sth->fetchall_arrayref({}, $count);
  $self->remark({ level => 3,
		  message => 'Got ' . scalar(@$rows) . ' results' });
  $rows;
}

sub store_chunk {
  my($self, $qry, $data, $opts) = @_;
  return unless $qry and $data;
  $opts //= {};
  my(@cols) = $opts->{column_names} // @{ $qry->sth->{NAME} };

  if (not @cols) {
    # Driver doesn't implement NAME for insert/update
    my $cache = $self->_qry_info;
    if ($cache->{"$qry"}->{columns}) {
      @cols = @{ $cache->{"$qry"}->{columns} };
    }
    else {
      # The hard way
      require SQL::Parser;
      state $p = SQL::Parser->new;
      if ($p->parse($qry->sth->{Statement})) {
	@cols = map { $_->{value} } @{ $p->structure->{column_defs} };
      }
      $cache->{"$qry"}->{columns} = \@cols;
    }
  }
  
  my(@values) = map { @{ $_ }{@cols} } @$data;
  my $rows = $qry->execute(@values)->rows;
  $self->logger->warn("store_chunk returned $rows rows with " .
		      scalar(@$data) . ' rows input')
    unless $rows == @$data;
  $rows;
}


1;

__END__

