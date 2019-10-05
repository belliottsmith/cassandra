FROM docker.apple.com/piedb/jdk1.8:latest

ARG CASSANDRA_VERSION

COPY .dist/publishable/com/apple/cie/db/cassandra/cie-cassandra/$CASSANDRA_VERSION/cie-cassandra-$CASSANDRA_VERSION-bin.tar.gz /
RUN tar -xvf cie-cassandra-$CASSANDRA_VERSION-bin.tar.gz
RUN ln -s cie-cassandra-$CASSANDRA_VERSION cassandrajar

RUN rpm -i http://repo-active/mrepo/oel6-x86_64/RPMS.cassandra/jemalloc-3.6.0-1.el6.x86_64.rpm
RUN rpm -i http://repo-active/mrepo/oel6-x86_64/RPMS.cassandra/jemalloc-devel-3.6.0-1.el6.x86_64.rpm

# remember to update this to /work/cassandrastubfourzero/cassandrastubfourzero.jar when cassandra moves to 4.0
ENV EXTRA_CLASSPATH /work/cassandrastubthreezero/cassandrastubthreezero.jar
ENV LAUNCHED_BY_WD40=true