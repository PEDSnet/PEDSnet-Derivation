#!perl
#
# $Id$

use 5.010;
use strict;
use warnings;

package PEDSnet::Derivation;

our($VERSION) = '0.01';
our($REVISION) = '$Revision$' =~ /: (\d+)/ ? $1 : 0;

use Module::Runtime qw( use_module );

use Moo 2;
use Types::Standard qw/InstanceOf Int HashRef/;

has 'src_backend' => (isa => InstanceOf['PEDSnet::Derivation::Backend'], is => 'ro',
		      required => 1 );

has 'sink_backend' => (isa => InstanceOf['PEDSnet::Derivation::Backend'], is => 'ro',
		       required => 1 );

has 'config' => (isa => InstanceOf['PEDSnet::Derivation::Config'],
		 is => 'ro', required => 1, lazy => 1,
		 builder => 'build_config');

sub build_config {
  my $self = shift;
  my $class = ref $self || $self;
  my $config_class = $class . '::Config';
  
  use_module($config_class)->new(derivation => $self);
}


1;

__END__

=head1 NAME

PEDSnet::Derivation - Utilities for creating derived variables in the PEDSnet CDM

=head1 DESCRIPTION

This module is a base class; for documentation of each tool please
see its specific module.

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

