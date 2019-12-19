#!/usr/bin/env bash
#
# This script tries to reproduce the logic from dna/src/com/apple/cie/db/dna/logic/LogicUtils.java under cie-db,
# warts and all.
#
set -o errexit
set -o nounset
set -o pipefail

readonly CARNIVAL_COMPATIBLE_NAME="$(dirname "$0")/carnival-compatible-name.sh"

# Limit stub version to first two sets of version numbers, and remove the dashes
function getStubVersionName {
    local -r cassandraVersion="$1"
    local -r stubVersion="$(echo "${cassandraVersion}" | sed -n 's/^\([1-9][0-9]*.[0-9]*\).*/\1/p')"
    if [ -s "${stubVersion}" ]
    then
        echo
        echo "Stub version requires at least major.minor"
        echo
        exit 2
    fi

    $CARNIVAL_COMPATIBLE_NAME "$stubVersion" | sed -e 's/-//g'
}

if [ -n "${1:-}" ]
then
    getStubVersionName "$1"
else
    echo
    echo "Requires a single argument of the cassandra version to convert, for example."
    echo
    echo "  \$ ${0} 3.0.19.0" >&2
    exit 2
fi
