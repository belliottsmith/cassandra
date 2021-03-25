# Image defined here: https://github.pie.apple.com/pie/cassandra-automation/blob/develop/dockerimages/jdkbaseimage/Dockerfile-cpbuild7-JDK8Base
FROM docker.apple.com/piedb7/applejdk-8:latest

MAINTAINER  ACI-Cassandra <aci-cassandra@group.apple.com>

ARG ARTIFACTS_VERSION
ARG STUB_JAR

COPY .dist/publishable/com/apple/cie/db/cassandra/cie-cassandra/$ARTIFACTS_VERSION/cie-cassandra-$ARTIFACTS_VERSION-bin.tar.gz /
RUN \
  tar -xvf cie-cassandra-$ARTIFACTS_VERSION-bin.tar.gz && \
  ln -s cie-cassandra-$ARTIFACTS_VERSION cassandrajar && \
  rm cie-cassandra-$ARTIFACTS_VERSION-bin.tar.gz

ENV EXTRA_CLASSPATH ${STUB_JAR}
ENV LAUNCHED_BY_WD40=true
