#!perl

use 5.010;
use strict;
use warnings;

package PEDSnet::Derivation::Backend::CSV;

our($VERSION) = '0.01';

=head1 NAME

PEDSnet::Derivation::Backend::CSV - Use CSV files for derivations

=head1 SYNOPSIS

  package My::Derivation;

  use PEDSnet::Derivation;
  use PEDSnet::Derivation::Backend::CSV;

  my $src =
     PEDSnet::Derivation::Backend::CSV->new(db_dir => '/my/source/data');
  my $sink =
     PEDSnet::Derivation::Backend::CSV->new(db_dir => '/my/derived/data');

  my $der = My::Derivation->new( src_backend => $src, sink_backend => $sink);
  ...

=head1 DESCRIPTION

This L<PEDSnet::Derivation::Backend> subclass mediates access to data
in CSV files.  It's intended to provide a widely-available, "least
common denominator" option for accessing data. For consistency, it
tries to make most operations look like they're addressing a
relational database, Following L<DBD::CSV>'s convention that a
directory is the database and individual CSV files are tables.
Rememeber, though, that L<DBD::CSV> has a fairly limited SQL
vocabulary and no real column typing, so you need to keep your
relational expectations low if you intend to support this backend.

=cut

# Internal package connecting to CSV driver
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

=head2 Attributes

The link to CSV file(s) with which the backend deals is established by
specifying either of two attributes:

=over 4

=item db_dir

A string specifting the path to the directory containing the CSV
files.  It will be passed unmodified to L<DBD::CSV/f_dir>, and hence
will default to the current working directory.

=cut

has 'db_dir' => (isa => Str, is => 'ro', required => 1 );

=item rdb

A L<Rose::DB>-derived object that manages the L<DBD>>CSV> connection
tobe used.  If this is specified, the value of L</db_dir> is ignored.

=cut

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

=back

In addition, this class consumes L<MooX::Role::Chatty>, so you may use
its logging attributes.

=head2 Methods

=over 4

=item column_names($table)

Return the names of the columns in I<$table>.  In scalar context,
returns the number of columns.

=cut

sub column_names {
  my($self,$table) = @_;
  my $dbh = $self->rdb->dbh;
  
  my $sth = $dbh->prepare('SELECT * FROM ' . $dbh->quote_identifier($table) .
			  ' LIMIT 1');
  $sth->execute;
  my(@cols) = @{ $sth->{NAME} };
  $sth->finish;
  @cols;
}

=item clone_table($src, $dest)

Create a new (empty) table named I<$dest> with the same structure as
the table named I<$src>.  Note that this occurs I<within> the backend;
it does not support creating a table in this backend based on the
structure of a table in another backend.

Returns I<$dest> on success, and nothing on failure.

=cut

sub clone_table {
  my($self,$src,$dest) = @_;
  $self->remark({ level => 2,
		  message => "Cloning structure of $src to $dest" });
  my $dbh = $self->rdb->dbh;
  return unless
    defined $dbh->do('CREATE TABLE ' . $dbh->quote_identifier($dest) .
		     ' AS SELECT * FROM ' . $dbh->quote_identifier($src) .
		     ' LIMIT 1');
  return unless $dbh->do('DELETE FROM ' . $dbh->quote_identifier($dest));
  return $dest;
}

=item build_query($sql)

=item get_query($sql)

Create a new L<Rose::DBx::CannedQuery::Glycolylated> using I<$sql> as
the query string.  Logging parameters are taken from the invocant, as
is the target database.  If L<build_uqery> is called, a new query is
returned each time; L</get_query> will return a cached query, if one
exists. 

=cut

sub _can_it {
  my($self, $method, $sql) = @_;
  Rose::DBx::CannedQuery::Glycosylated->$method(rdb => $self->rdb,
						sql => $sql,
						verbose => $self->verbose,
						logger => $self->logger);
}

sub build_query { shift->_can_it('new', @_);}
sub get_query   { shift->_can_it('new_or_cached', @_);}

=item execute($query, $params)

Executes I<$query>, which was constructed using L</build_query> or
L</get_query>.  If present, I<$params> must be a reference to an array
of bind parameters.

If successful, returns an active L<DBI> statement handle.  For
mainline data handling, you'll probably be better off using
L</fetch_chunk> or </store_chunk> than fetching results through this
handle directly.  But interacting with the statement handle may be
useful for diagnostics or status checks when doing setup.

On failure, returns nothing, and generates a warning.

At verbosity level 2, outputs a log message with bind parameter values.

=cut

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

=item fetch_chunk($query, $count)

Retrieves up to I<$count> rows of data from an L</execute>d
I<$query>.

Returns a reference to an array of hashes containing the
resultset. 

At verbosity level 3, outputs a message indicating how many rows were
fetched. 

=cut

sub fetch_chunk {
  my($self, $qry, $count) = @_;
  my $sth = $qry->sth;
  return [] unless $sth and $sth->{Active};
  my $rows = $sth->fetchall_arrayref({}, $count);
  $self->remark({ level => 3,
		  message => 'Got ' . scalar(@$rows) . ' results' });
  $rows;
}

=item store_chunk($query, $data)

Writes out the contents of I<$data> using I<$query>.  The structure of
I<$data> is similar to that in L</fetch_chunk>: a reference to an
array of rows, each of which is a hash reference of column names and
values.

Column names are looked up in I<$query>, and for each row in I<$data>,
the relevant columns are extracted, and I<$query> is executed with
those values as bind parameters.  (As a word to the wise, remember
that L<Rose::DBx::CannedQuery> downcases column names by default when
fetching data; make sure the keys in I<$data> and the column names in
I<$query> are matched.)

Returns the number of rows written.  Generates a warning if that's not
the same as the number of rows in I<$data>.

=cut

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
