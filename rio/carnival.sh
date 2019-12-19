#!/bin/bash

set -xe

echo === START CARNIVAL BUILD === 

# There is a bug in workflows that assumes lower casing in the version, even -SNAPSHOT must be -snapshot; this causes issues with Artifactory as it requires -SNAPSHOT.
# In order to work around the assumption, we need to make the carnival package use lower casing for the version, even though the rest of the builds use normal casing
# see rdar://65381303 (Workflows puts a lowercase restriction on carnival name and fails if cluster version uses capital letters)
VERSION="$(echo "$1" | tr '[[:upper:]]' '[[:lower:]]')"
CARNIVAL_VERSION_NAME="$2"
BUILD_VERSION="$3"

# TODO - work out if Carnival really needs differently versioned artifacts than the main snapshot release - i.e. can it handle 4.0.0.0 vs 4.0.0

export JAVA_TOOL_OPTIONS=-Dfile.encoding=UTF8
ant -f rio-build.xml realclean
ant -f rio-build.xml -Drelease=true -Dbase.version="$VERSION" clean artifacts

DIST_DIR=.carnival

rm -rf "$DIST_DIR" || true
mkdir -p "$DIST_DIR"

tar -xzvf "build/cie-cassandra-${VERSION}-bin.tar.gz" -C "$DIST_DIR"
mv "$DIST_DIR/cie-cassandra-${VERSION}"/* "$DIST_DIR/"
rm -rf "$DIST_DIR/cie-cassandra-${VERSION}"
rm -rf "$DIST_DIR/javadoc"

touch "$DIST_DIR/.application"

echo "Not cassandra jar. Just a placeholder for rio scripts. Look at lib/cie-cassandra-${VERSION}.jar, instead." > $DIST_DIR/cie-cassandra-${VERSION}.jar

# v4 package template requires call to stage the app
find "$DIST_DIR" -ls
ci stage-app --app-name "cie-cassandra-${CARNIVAL_VERSION_NAME}" --app-version "${BUILD_VERSION}" "${DIST_DIR}/**"
# find .cicd -ls

echo === COMPLETE CARNIVAL BUILD === 

set +xe
