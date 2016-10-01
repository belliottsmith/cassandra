#!/bin/bash

set -xe

MAJOR_VERSION=$1

# if git describe --tags --long --match "${MAJOR_VERSION}"; then
#     PREV_MIN_V=$(git describe --tags --match "${MAJOR_VERSION}" | sed -e "s/${MAJOR_VERSION}.//")
# else
#     PREV_MIN_V=0
# fi
# 
# VERSION=$MAJOR_VERSION.$((PREV_MIN_V+1))
VERSION=$MAJOR_VERSION

export JAVA_TOOL_OPTIONS=-Dfile.encoding=UTF8
ant -f rio-build.xml -Drelease=true -Dbase.version=${VERSION} mvn-install

GROUP_DIR=.dist/publishable/com/apple/cie/db/cassandra

# Parent POM file
mkdir -p ${GROUP_DIR}/cassandra-parent/${VERSION}

cp build/cie-cassandra-${VERSION}-parent.pom ${GROUP_DIR}/cassandra-parent/${VERSION}/cassandra-parent-${VERSION}.pom

# Thrift artifact
mkdir -p ${GROUP_DIR}/cassandra-thrift/${VERSION}

cp build/cie-cassandra-thrift-${VERSION}.jar ${GROUP_DIR}/cassandra-thrift/${VERSION}/cassandra-thrift-${VERSION}.jar
cp build/cie-cassandra-thrift-${VERSION}.pom ${GROUP_DIR}/cassandra-thrift/${VERSION}/cassandra-thrift-${VERSION}.pom
cp build/cie-cassandra-thrift-${VERSION}-sources.jar ${GROUP_DIR}/cassandra-thrift/${VERSION}/cassandra-thrift-${VERSION}-sources.jar
cp build/cie-cassandra-thrift-${VERSION}-javadoc.jar ${GROUP_DIR}/cassandra-thrift/${VERSION}/cassandra-thrift-${VERSION}-javadoc.jar


# Client util artifact
mkdir -p ${GROUP_DIR}/cassandra-clientutil/${VERSION}

cp build/cie-cassandra-clientutil-${VERSION}.jar ${GROUP_DIR}/cassandra-clientutil/${VERSION}/cassandra-clientutil-${VERSION}.jar
cp build/cie-cassandra-clientutil-${VERSION}.pom ${GROUP_DIR}/cassandra-clientutil/${VERSION}/cassandra-clientutil-${VERSION}.pom
cp build/cie-cassandra-clientutil-${VERSION}-sources.jar ${GROUP_DIR}/cassandra-clientutil/${VERSION}/cassandra-clientutil-${VERSION}-sources.jar
cp build/cie-cassandra-clientutil-${VERSION}-javadoc.jar ${GROUP_DIR}/cassandra-clientutil/${VERSION}/cassandra-clientutil-${VERSION}-javadoc.jar


# Cassandra artifact
mkdir -p ${GROUP_DIR}/cassandra-all/${VERSION}

cp build/cie-cassandra-${VERSION}.jar ${GROUP_DIR}/cassandra-all/${VERSION}/cassandra-all-${VERSION}.jar
cp build/cie-cassandra-${VERSION}.pom ${GROUP_DIR}/cassandra-all/${VERSION}/cassandra-all-${VERSION}.pom
cp build/cie-cassandra-${VERSION}-sources.jar ${GROUP_DIR}/cassandra-all/${VERSION}/cassandra-all-${VERSION}-sources.jar
cp build/cie-cassandra-${VERSION}-javadoc.jar ${GROUP_DIR}/cassandra-all/${VERSION}/cassandra-all-${VERSION}-javadoc.jar

# Cassandra artifact
mkdir -p ${GROUP_DIR}/cie-cassandra/${VERSION}

cp build/cie-cassandra-${VERSION}-bin.tar.gz ${GROUP_DIR}/cie-cassandra/${VERSION}/

set +xe
