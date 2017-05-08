#!perl


use 5.010;
use strict;
use warnings;

package PEDSnet::Derivation::Backend;

our($VERSION) = '0.01';
our($REVISION) = '$Revision$' =~ /: (\d+)/ ? $1 : 0;

1;


__END__

=head1 NAME

PEDSnet::Derivation::Backend - Base class for derivation backends

=head1 DESCRIPTION

This module is a base for classes implementing the linkage between the
computation of a derived variable and the data store from which input
is taken (referred to as the "source backend") or output is written
(referred to as the "sink backend").  For documentation of each
backend type please see its specific module.

There are intentionally no required capabilities for all
L<PEDSnet::Derviation::Backend>s, in order to allow flexibility to
meet the needs specific needs of different derivations.  However, to
be useful, you will probably want you backend to do at least some of
the following:

=over 4

=item *

Retrieve specific data (source backends)

=item *

Retrieve data other than that involved in computing the derived values
(e.g. counts or ranges used to prepare tasks) (source backend)

=item *

Write specific data (sink backends)

=item *

Create a container into which to put data (sink backends)

=item *

Mutate data (combined backends)

=item *

Retrieve some metadata about the data source (e.g. schema)

=back

=head1 BUGS AND CAVEATS

Are there, for certain, but have yet to be cataloged.

=head1 REVISION

$Revision$ $Date$

=head1 AUTHOR

Charles Bailey, E<lt>baileyc@email.chop.edu<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2015 by Charles Bailey

=cut

