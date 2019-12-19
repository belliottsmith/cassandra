#!/usr/bin/env bash

set -o nounset
set -o errexit
set -o pipefail

cat "$(dirname "$0")/../build.xml" | grep -Ei 'artifactId=\"dtest-api\" version=\"(.*)\"'  | awk -F "\"" '{print $6}'
