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
ant -f rio-build.xml -Drelease=true -Dbase.version=${VERSION} clean mvn-install

DIST_DIR=.dist

mkdir -p $DIST_DIR

tar -xzvf build/cie-cassandra-${VERSION}-bin.tar.gz -C $DIST_DIR
mv $DIST_DIR/cie-cassandra-${VERSION}/* $DIST_DIR/
rm -rf cie-cassandra-${VERSION}

touch $DIST_DIR/.application

echo "Not cassandra jar. Just a placeholder for rio scripts. Look at lib/cie-cassandra-${VERSION}.jar, instead." > $DIST_DIR/cie-cassandra-${VERSION}.jar

set +xe
