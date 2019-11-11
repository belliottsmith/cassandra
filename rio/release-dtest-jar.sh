#!/bin/bash

set -xe

export REVISION=`git rev-parse --short HEAD`
export VERSION=$1
export ARTIFACT_NAME=cassandra-dtest
export REPO_DIR=.dist/

cd /workspace
cp ./rio/patches/cassandra/build.properties.default ./
ant clean
ant dtest-jar

# Install the version that will be shaded
mvn install:install-file               \
   -Dfile=./build/dtest-${VERSION}.jar \
   -DgroupId=com.apple.cie.db          \
   -DartifactId=${ARTIFACT_NAME}-local \
   -Dversion=${VERSION}                \
   -Dpackaging=jar                     \
   -DgeneratePom=true

# Create shaded artifact
mvn -f relocate-dependencies.pom package -DskipTests -nsu

# Deploy shaded artifact
mvn install:install-file                                 \
   -Dfile=./.dist/${ARTIFACT_NAME}-shaded-${VERSION}.jar \
   -DgroupId=com.apple.cie.db                            \
   -DartifactId=${ARTIFACT_NAME}-shaded                  \
   -Dversion=${VERSION}-${REVISION}                      \
   -Dpackaging=jar                                       \
   -DgeneratePom=true                                    \
   -DlocalRepositoryPath=${REPO_DIR}

# Deploy the unshaded artifact
mvn install:install-file               \
   -Dfile=./build/dtest-${VERSION}.jar \
   -DgroupId=com.apple.cie.db          \
   -DartifactId=${ARTIFACT_NAME}       \
   -Dversion=${VERSION}-${REVISION}    \
   -Dpackaging=jar                     \
   -DgeneratePom=true                  \
   -DlocalRepositoryPath=${REPO_DIR}

# Cleanup
rm ./.dist/*.jar
find ./ -name maven-metadata-local.xml -print0 | xargs -0 rm

find ./.dist

ci stage-lib "/workspace/.dist/**"

set +xe
