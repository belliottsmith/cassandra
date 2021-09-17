#!/usr/bin/env bash
#
# Output the contents of the CIE-VERSION file so Rio can use it in variable interpolation
#
set -o nounset
set -o errexit
set -o pipefail


cat "$(dirname "$0")/../CIE-VERSION"
