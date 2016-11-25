#!perl
#
# $Id$

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

=head1 BUGS AND CAVEATS

Are there, for certain, but have yet to be cataloged.

=head1 REVISION

$Revision$ $Date$

=head1 AUTHOR

Charles Bailey, E<lt>baileyc@email.chop.edu<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2015 by Charles Bailey

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.10.0 or,
at your option, any later version of Perl 5 you may have available.

=cut

