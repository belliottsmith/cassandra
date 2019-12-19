#!/bin/bash

set -xe

echo === START CARNIVAL BUILD === 

VERSION="$1"
CARNIVAL_VERSION_NAME="$2"
BUILD_VERSION="$3"

# TODO - work out if Carnival really needs differently versioned artifacts than the main snapshot release - i.e. can it handle 4.0.0.0 vs 4.0.0

export JAVA_TOOL_OPTIONS=-Dfile.encoding=UTF8
ant -f rio-build.xml -Drelease=true -Dbase.version=${VERSION} clean artifacts

DIST_DIR=.carnival

mkdir -p $DIST_DIR

tar -xzvf build/cie-cassandra-${VERSION}-bin.tar.gz -C $DIST_DIR
mv $DIST_DIR/cie-cassandra-${VERSION}/* $DIST_DIR/
rm -rf cie-cassandra-${VERSION}
rm -rf "$DIST_DIR/javadoc"

touch $DIST_DIR/.application

echo "Not cassandra jar. Just a placeholder for rio scripts. Look at lib/cie-cassandra-${VERSION}.jar, instead." > $DIST_DIR/cie-cassandra-${VERSION}.jar

# v4 package template requires call to stage the app
find $DIST_DIR -ls
ci stage-app --app-name "cie-cassandra-${CARNIVAL_VERSION_NAME}" --app-version "${BUILD_VERSION}" "${DIST_DIR}/**"
# find .cicd -ls

echo === COMPLETE CARNIVAL BUILD === 

set +xe
