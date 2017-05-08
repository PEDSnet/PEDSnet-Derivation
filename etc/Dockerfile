# Minimal Dockerfile for PEDSnet::Derivation build, using standard
# Perl image as base
#
# To set up, just pick the build option you want below, then say
#   docker build -t PEDSnet-Derivation -f /path/to/this/file
#
# Once built, you can run with access to files in /your/data/dir
# by saying
#   docker run -v /your/data/dir:/working --rm -it PEDSnet-Derivation

FROM perl:latest
LABEL description "Simple image for PEDSnet::Derivation"
LABEL maintainer "Charles Bailey <baileyc@email.chop.edu>"

# Build using the current version on GitHub
RUN cpanm git://github.com/PEDSnet/PEDSnet-Derivation

# Build using released version of PEDSnet::Derivation
#RUN cpanm PEDSnet::Derivation

# Alternatively, to install local copy, first say
#   dzil build
# then build using local version
#COPY PEDSnet-Derivation-*.tar.gz /src/
#RUN cpanm /src/*.tar.gz


RUN useradd -m -s /bin/bash normalized
USER normalized
WORKDIR /working
CMD /bin/bash
