#! /usr/bin/bash
set -o nounset
set -o errexit
set -o pipefail

if [ -f rio-build.xml ]
then
  ant -f rio-build.xml -Dbase.version=$(<CIE-VERSION) dtest-jar
else
  ant dtest-jar
fi
