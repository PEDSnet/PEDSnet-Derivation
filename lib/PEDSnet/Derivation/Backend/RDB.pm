#!perl

use 5.010;
use strict;
use warnings;

package PEDSnet::Derivation::Backend::RDB;

our($VERSION) = '0.01';

=head1 NAME

PEDSnet::Derivation::Backend::RDB - Use Rose::DB for derivations

=head1 SYNOPSIS

  package My::Derivation;

  use PEDSnet::Derivation;
  use PEDSnet::Derivation::Backend::RDB;

  use Rose::DBx::MoreConfig;

  my $input_rdb =
     Rose::DBx::MoreConfig->new(type => 'src_db', domain => 'test')
  my $output_rdb =
     Rose::DBx::MoreConfig->new(type => 'dest_db', domain => 'test')
  my $src =
     PEDSnet::Derivation::Backend::CSV->new(rdb => $input_rdb);
  my $sink =
     PEDSnet::Derivation::Backend::CSV->new(db_dir => $output_rdb);

  my $der = My::Derivation->new( src_backend => $src, sink_backend => $sink);
  ...

=head1 DESCRIPTION

This L<PEDSnet::Derivation::Backend> subclass mediates access to
relational databases via L<Rose::DB>-derived objects managing the
database connections.  The use of L<Rose::DB> abstracts out some
variation among DBMSs, as well as low-level connection management.

=cut

use Scalar::Util qw/ blessed reftype /;

use Moo 2;
use Types::Standard qw/ InstanceOf HashRef /;

use Rose::DBx::CannedQuery::Glycosylated;

extends 'PEDSnet::Derivation::Backend';
with 'MooX::Role::Chatty';

=head2 Attributes

The database is specified by setting a single attribute:

=over 4

=item rdb

A L<Rose::DB>-derived object describing the database connection.  You
may initialize this in one of three ways:

=over 4

=item *

by passing in an already-constructed object

=item *

by passing in a hash reference with C<type> and C<domain> keys, that
will be passed to L<Rose::DBx::MoreConfig>'s
L<Rose::DBx::MoreConfig/new> constructor to create the object

=item *

by passing in a string containing a L<DBI> DSN (cf. L<parse_dsn/DBI>)
from which an attempt is made to construct a new L<Rose::DB> object.
The string must start with C<dbi:> to be recognized as such.

As an extra convenience, if the string contains a C<schema=>I<name>
tag, it's assumed that this describes a Postgres search path, and
creates a L<Rose::DB/post_connect_sql> statement to set C<search_path>
to I<name>.

=back

=cut

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
  my($src_id, $dest_id);

  # This dance is necessary to quote schema and table names separately
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

=item build_query($sql)

=item get_query($sql)

Create a new L<Rose::DBx::CannedQuery::Glycolylated> using I<$sql> as
the query string.  Logging parameters are taken from the invocant, as
is the target database.  If L<build_uqery> is called, a new query is
returned each time; L</get_query> will return a cached query, if one
exists. 

=cut

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
I<$query> are matched.)  Some DBD drivers don't support introspection
on inserts or updates, in which case a best effort is made to figure
out the column names using L<SQL::Parser>.  Hopefully, this won't
affect you, but if you see mismatches between your SQL and what's
stored, this is something to check.

Returns the number of rows written.  Generates a warning if that's not
the same as the number of rows in I<$data>.

=cut

sub store_chunk {
  my($self, $qry, $data, $slice) = @_;
  return unless $qry and $data;
  my(@cols) = @{ $slice // $qry->sth->{NAME} // [] };

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
      if (keys %$cache > 8) {
	# Crude strategy to keep cache from expanding rapidly
	# in app that uses many different inserts
	my @keys = keys %$cache;
	my $idx = $keys[int(rand @keys)];
	delete $cache->{$idx};
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
