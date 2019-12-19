#!/usr/bin/env bash

set -o nounset
set -o errexit
set -o pipefail

cat build.xml | grep 'property name="base.version"' | awk -F "\"" '{print $4}'
