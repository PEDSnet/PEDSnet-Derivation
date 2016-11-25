#!perl
#
# $Id$

use 5.010;
use strict;
use warnings;

package PEDSnet::Derivation::Backend::CSV;

our($VERSION) = '0.01';
our($REVISION) = '$Revision$' =~ /: (\d+)/ ? $1 : 0;

package
  PEDSnet::Derivation::Backend::CSV::_RDB;

use parent 'Rose::DB';
__PACKAGE__->use_private_registry;
__PACKAGE__->register_db(domain => 'dummy',
			 type => 'dummy',
			 driver => 'CSV',
			 username => undef,
			 password => undef);
__PACKAGE__->default_domain('dummy');
__PACKAGE__->default_type('dummy');

package PEDSnet::Derivation::Backend::CSV;

use Scalar::Util qw/ reftype /;

use Moo 2;
use Types::Standard qw/ Str InstanceOf /;

use Rose::DBx::CannedQuery::Glycosylated;

extends 'PEDSnet::Derivation::Backend';
with 'MooX::Role::Chatty';

has 'db_dir' => (isa => Str, is => 'ro', required => 1 );

has 'rdb' => ( isa => InstanceOf['Rose::DB'], is => 'ro',
	       required => 1, lazy => 1, builder => '_build_rdb' );

sub _build_rdb {
  my $rdb = Rose::DB->new(driver => 'CSV');
  $rdb->dsn('dbi:CSV:');
  $rdb->connect_options( { f_dir => shift->db_dir,
			   csv_eol => "\n",
			   csv_binary => 1,
			   csv_empty_is_undef => 1,
			   csv_allow_loose_escapes => 1,
			   csv_auto_diag => 1 });
  $rdb;
}

sub column_names {
  my($self,$table) = @_;
  my $dbh = $self->rdb->dbh;
  
  my $sth = $dbh->prepare('SELECT * FROM ' . $dbh->quote_identifier($table) .
			  ' LIMIT 1,1');
  $sth->execute;
  my(@cols) = @{ $sth->{NAME} };
  $sth->finish;
  @cols;
}

sub clone_table {
  my($self,$src,$dest) = @_;
  $self->remark({ level => 2,
		  message => "Cloning structure of $src to $dest" });
  my $dbh = $self->rdb->dbh;
  return unless
    defined $dbh->do('CREATE TABLE ' . $dbh->quote_identifier($dest) .
		     ' AS SELECT * FROM ' . $dbh->quote_identifier($src) .
		     ' LIMIT 1,1');
  return unless $dbh->do('DELETE FROM ' . $dbh->quote_identifier($dest));
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
  $rv->rows;
}
  
sub fetch_chunk {
  my($self, $qry, $count) = @_;
  my @rows = $qry->sth->fetchall_arrayref({}, $count);
  $self->remark({ level => 3,
		  message => 'Got ' . scalar(@rows) . ' results' });
  @rows;
}

sub store_chunk {
  my($self, $qry, $data) = @_;
  return unless $qry and $data;
  my(@cols) = @{ $qry->sth->{NAME} };
  my(@values) = map { @{ $_ }{@cols} } @$data;
  my $rows = $qry->execute(@values)->rows;
  $self->logger->warn("store_chunk returned $rows rows with " .
		      scalar(@$data) . ' rows input')
    unless $rows == @$data;
  $rows;
}


1;

__END__

