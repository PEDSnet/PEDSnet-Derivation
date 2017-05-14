#!perl


use 5.010;
use strict;
use warnings;

package PEDSnet::Derivation;

our($VERSION) = '0.01';

use Module::Runtime qw( use_module );

use Moo 2;
use Types::Standard qw/InstanceOf Int HashRef/;

=head1 NAME

PEDSnet::Derivation - Base class for creating derived variables in the PEDSnet CDM

=head1 DESCRIPTION

The L<PEDSnet::Derivation> framework provides a simple structure for
operating on data, typically in the PEDSnet CDM, to derive new
information.  The framework is agnostic about the details of the
derivation; it simply models a flow of data from a source backend, to
a sink backend.

L<PEDSnet::Derivation> itself is a base class, which specifies three
generic attributes:

=head2 Attributes

=over 4

=item src_backend

An object conforming to L<PEDSnet::Derivation::Backend>, that supports
retrieval of the data needed for the derivation.

=cut

has 'src_backend' => (isa => InstanceOf['PEDSnet::Derivation::Backend'], is => 'ro',
		      required => 1 );

=item sink_backend

An object conforming to L<PEDSnet::Derivation::Backend>, that supports
recording of the derived data.

=cut

has 'sink_backend' => (isa => InstanceOf['PEDSnet::Derivation::Backend'], is => 'ro',
		       required => 1 );

=item config

A L<PEDSnet::Derivation::Config> object encapsulating configuration
parameters to be used in the transformation from source to derived
data. 

=cut

has 'config' => (isa => InstanceOf['PEDSnet::Derivation::Config'],
		 is => 'ro', required => 1, lazy => 1,
		 builder => 'build_config');

=back

=head2 Methods

A single public method is provided to help acquire configuration data:

=over 4

=item build_config

This builder method attempts to load a module named for the caller's
class, with C<::Config> appended.  For example, if called via a
C<My::Derivation> object, it will attempt to load
C<My::Derivation::Config>. 

If successful, returns a configuration object is constructed with the
single attribute C<derivation> set to the invocant.  Otherwise, an
exception is raised.

The overall result is that C<<My::Derivation->new->config>> will
automatically load and instantiate a C<My::Derivation::Config> object.

=cut


sub build_config {
  my $self = shift;
  my $class = ref $self || $self;
  my $config_class = $class . '::Config';
  
  use_module($config_class)->new(derivation => $self);
}


1;

__END__

=back

=head1 BUGS AND CAVEATS

Are there, for certain, but have yet to be cataloged.

=head1 AUTHOR

Charles Bailey, E<lt>baileyc@email.chop.edu<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2015 by Charles Bailey

This software may be used under the terms of the Artistic License or
the GNU General Public License, as the user prefers.

This code was written at the Children's Hospital of Philadelphia as
part of L<PCORI|http://www.pcori.org>-funded work in the
L<PEDSnet|http://www.pedsnet.org> Data Coordinating Center.

=cut

