
use strict;
use warnings;

use Module::Build 0.28;


my %module_build_args = (
  "build_requires" => {
    "Module::Build" => "0.28"
  },
  "configure_requires" => {
    "ExtUtils::MakeMaker" => 0,
    "Module::Build" => "0.28"
  },
  "dist_abstract" => "Base class for creating derived variables in the PEDSnet CDM",
  "dist_author" => [
    "Charles Bailey <cbail\@cpan.org>"
  ],
  "dist_name" => "PEDSnet-Derivation",
  "dist_version" => "0.01",
  "license" => "open_source",
  "module_name" => "PEDSnet::Derivation",
  "recursive_test_files" => 1,
  "requires" => {
    "Config::Any" => 0,
    "FindBin" => 0,
    "Module::Runtime" => 0,
    "Moo" => 2,
    "MooX::Role::Chatty" => 0,
    "Path::Tiny" => 0,
    "Rose::DB" => 0,
    "Rose::DBx::CannedQuery" => 0,
    "Rose::DBx::CannedQuery::Glycosylated" => 0,
    "SQL::Parser" => 0,
    "Scalar::Util" => 0,
    "Types::Standard" => 0,
    "parent" => 0,
    "perl" => "5.010",
    "strict" => 0,
    "warnings" => 0
  },
  "test_requires" => {
    "DBD::CSV" => 0,
    "DBD::SQLite" => 0,
    "ExtUtils::MakeMaker" => 0,
    "File::Spec" => 0,
    "File::Temp" => 0,
    "Rose::DBx::MoreConfig" => 0,
    "Test::More" => 0
  }
);


my %fallback_build_requires = (
  "DBD::CSV" => 0,
  "DBD::SQLite" => 0,
  "ExtUtils::MakeMaker" => 0,
  "File::Spec" => 0,
  "File::Temp" => 0,
  "Module::Build" => "0.28",
  "Rose::DBx::MoreConfig" => 0,
  "Test::More" => 0
);


unless ( eval { Module::Build->VERSION(0.4004) } ) {
  delete $module_build_args{test_requires};
  $module_build_args{build_requires} = \%fallback_build_requires;
}

my $build = Module::Build->new(%module_build_args);


$build->create_build_script;
