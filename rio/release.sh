#!/usr/bin/env bash

set -xe

echo === the build env as rio/release.sh sees it ===
env
echo === and on with the build

MAJOR_VERSION="$1"

# if git describe --tags --long --match "${MAJOR_VERSION}"; then
#     PREV_MIN_V=$(git describe --tags --match "${MAJOR_VERSION}" | sed -e "s/${MAJOR_VERSION}.//")
# else
#     PREV_MIN_V=0
# fi
# 
# VERSION=$MAJOR_VERSION.$((PREV_MIN_V+1))
VERSION="$MAJOR_VERSION"

export JAVA_TOOL_OPTIONS=-Dfile.encoding=UTF8
ant -f rio-build.xml -Drelease=true -Dbase.version="${VERSION}" realclean
ant -f rio-build.xml -Drelease=true -Dbase.version="${VERSION}" -Dno-javadoc=true artifacts dtest-jar sources-jar sbom
ant -f rio-build.xml -Drelease=true -Dbase.version="${VERSION}" -Dno-javadoc=true -Dno-checkstyle=true javadoc-jar || true

GROUP_DIR=.dist/publishable/com/apple/cie/db/cassandra

# Parent POM file
mkdir -p "${GROUP_DIR}/cassandra-parent/${VERSION}"

cp "build/cie-cassandra-${VERSION}-parent.pom" "${GROUP_DIR}/cassandra-parent/${VERSION}/cassandra-parent-${VERSION}.pom"

###No longer seems to be build as of a3e772b8b92b00a7acc86e5aac34743ba36bb2e9 / CASSANDRA-11635
### # Client util artifact
### mkdir -p ${GROUP_DIR}/cassandra-clientutil/${VERSION}
### 
### cp build/cie-cassandra-clientutil-${VERSION}.jar ${GROUP_DIR}/cassandra-clientutil/${VERSION}/cassandra-clientutil-${VERSION}.jar
### cp build/cie-cassandra-clientutil-${VERSION}.pom ${GROUP_DIR}/cassandra-clientutil/${VERSION}/cassandra-clientutil-${VERSION}.pom
### cp build/cie-cassandra-clientutil-${VERSION}-sources.jar ${GROUP_DIR}/cassandra-clientutil/${VERSION}/cassandra-clientutil-${VERSION}-sources.jar
### cp build/cie-cassandra-clientutil-${VERSION}-javadoc.jar ${GROUP_DIR}/cassandra-clientutil/${VERSION}/cassandra-clientutil-${VERSION}-javadoc.jar


# Cassandra artifact
mkdir -p "${GROUP_DIR}/cassandra-all/${VERSION}"

cp "build/cie-cassandra-${VERSION}.jar" "${GROUP_DIR}/cassandra-all/${VERSION}/cassandra-all-${VERSION}.jar"
cp "build/cie-cassandra-${VERSION}.pom" "${GROUP_DIR}/cassandra-all/${VERSION}/cassandra-all-${VERSION}.pom"
cp "build/cie-cassandra-${VERSION}-sources.jar" "${GROUP_DIR}/cassandra-all/${VERSION}/cassandra-all-${VERSION}-sources.jar"
# javadoc build may fail
cp "build/cie-cassandra-${VERSION}-javadoc.jar" "${GROUP_DIR}/cassandra-all/${VERSION}/cassandra-all-${VERSION}-javadoc.jar" || true
cp "build/dtest-${VERSION}.jar" "${GROUP_DIR}/cassandra-all/${VERSION}/dtest-${VERSION}.jar"

# Cassandra artifact
mkdir -p "${GROUP_DIR}/cie-cassandra/${VERSION}"

cp "build/cie-cassandra-${VERSION}-bin.tar.gz" "${GROUP_DIR}/cie-cassandra/${VERSION}/"

# Rio v4 freestyle publishing
echo Current working dir: `pwd` $PWD
find .dist -ls

find /workspace/.cicd/ -ls
ci stage-lib '.dist/publishable/**' -v5 --allow-external 
find /workspace/.cicd/ -ls

# Stage SBOM
ci stage-sbom --help
cat build/target/bom.json
ci stage-sbom build/target/bom.json

set +xe
