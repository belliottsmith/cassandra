# Image defined here: https://github.pie.apple.com/pie/cassandra-automation/blob/develop/dockerimages/jdkbaseimage/Dockerfile-JDKBase
FROM docker.apple.com/piedb/applejdk-8:alpha

MAINTAINER  ACI-Cassandra <aci-cassandra@group.apple.com>

ARG CASSANDRA_VERSION

COPY .dist/publishable/com/apple/cie/db/cassandra/cie-cassandra/$CASSANDRA_VERSION/cie-cassandra-$CASSANDRA_VERSION-bin.tar.gz /
RUN \
  tar -xvf cie-cassandra-$CASSANDRA_VERSION-bin.tar.gz && \
  ln -s cie-cassandra-$CASSANDRA_VERSION cassandrajar && \
  rm cie-cassandra-$CASSANDRA_VERSION-bin.tar.gz

# remember to update this to /work/cassandrastubfourzero/cassandrastubfourzero.jar when cassandra moves to 4.0
ENV EXTRA_CLASSPATH /work/cassandrastubthreezero/cassandrastubthreezero.jar
ENV LAUNCHED_BY_WD40=true
