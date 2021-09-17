#!/usr/bin/env bash
#
# Test script for all the Rio version handling scripts
#

readonly RIODIR="$(dirname "$0")"

function expectresult {
    local -r what="$1"
    local -r expected="$2"
    local -r got="$3"

    if [ "$expected" != "$got" ]
    then
        echo "Expected $what '$expected' != '$got'" >&2
        exit 1
    fi
}


function test-version {
    if [[ $# -ne 6 ]]; then
      echo "Wrong number of arguments to test-version; expected 6, given $#"
      exit 1
    fi
    local -r buildtype="$1"
    local -r branchname="$2"
    local -r cieversion="$3"
    local -r expectfull="$4"
    local -r expectstub="$5"
    local -r expectcvn="$6"

    local -r full=$("$RIODIR/version-tool.sh" "$buildtype" "$branchname" "$cieversion")
    local -r stub=$("$RIODIR/stub-version.sh" "$full")
    local -r cvn=$("$RIODIR/carnival-compatible-name.sh" "$full")
    local -r cs=$("$RIODIR/version-tool.sh" carnivalsuffix "$buildtype" "$branchname" "$cieversion")


    expectresult base "$expectbase" "$base"
    expectresult full "$expectfull" "$full"
    expectresult stub "$expectstub" "$stub"
    expectresult carnival-version-name "$expectcvn" "$cvn"

}

function test-fail {
    if [[ $# -ne 4 ]]; then
      echo "Wrong number of arguments to test-version; expected 4, given $#"
      exit 1
    fi
    local -r buildtype="$1"
    local -r branchname="$2"
    local -r cieversion="$3"
    local -r expected_error="$4"


    if full=$("$RIODIR/version-tool.sh" "$buildtype" "$branchname" "$cieversion" 2>&1 ) &&
            stub=$(./rio/stub-version.sh "$full" 2>&1 ) &&
            cvn=$(./rio/carnival-compatible-name.sh "$full" 2>&1 )
    then
        echo "Expected failure for" "$@" "but no all version commands exited zero" >&2
        exit 1
    fi
    if [[ "$full" == *"$expected_error"* ]]; then
      return 0
    fi
    if [[ "$stub" == *"$expected_error"* ]]; then
      return 0
    fi
    if [[ "$cvn" == *"$expected_error"* ]]; then
      return 0
    fi
    echo -e "Unable to find $expected_error in any of the failed outputs:\nversion-tool.sh: $full\nstub-version.sh: $stub\ncarnival-compatible-name.sh: $cvn" 1>&2
    exit 1
}

# There is a bug in workflows that assumes lower casing in the version, even -SNAPSHOT must be -snapshot; this causes issues with Artifactory as it requires -SNAPSHOT.
# In order to work around the assumption, we need to make the carnival package use lower casing for the version, even though the rest of the builds use normal casing
# see rdar://65381303 (Workflows puts a lowercase restriction on carnival name and fails if cluster version uses capital letters)

test-version release  cie-cassandra-3.0.19  3.0.19.0  3.0.19.0          threezero three-zero-nineteen-zero
test-version release  cie-cassandra-3.0.19  3.0.19.1  3.0.19.1          threezero three-zero-nineteen-one
test-version snapshot cie-cassandra-3.0.19  3.0.19.0  3.0.19.0-SNAPSHOT threezero three-zero-nineteen-zero-snapshot
test-version snapshot cie-cassandra-3.0.19  3.0.19.1  3.0.19.1-SNAPSHOT threezero three-zero-nineteen-one-snapshot

test-version release  cie-cassandra-3.0.19.0-hotfix 3.0.19.0-hotfix 3.0.19.0        threezero   three-zero-nineteen-zero
test-version release  cie-cassandra-3.0.19.0-hotfix 3.0.19.1-hotfix 3.0.19.1        threezero   three-zero-nineteen-one
test-version snapshot cie-cassandra-3.0.19.0-hotfix 3.0.19.0-hotfix 3.0.19.0-hotfix-SNAPSHOT threezero   three-zero-nineteen-zero-hotfix-snapshot
test-version snapshot cie-cassandra-3.0.19.0-hotfix 3.0.19.1-hotfix 3.0.19.1-hotfix-SNAPSHOT threezero   three-zero-nineteen-one-hotfix-snapshot

test-fail release cie-cassandra-3.0.19-custom 3.0.19.0-custom "Only hotfix version sufix supported for release and this build has version suffix 'custom'"
test-fail release cie-cassandra-3.0.19-custom 3.0.19.1-custom "Only hotfix version sufix supported for release and this build has version suffix 'custom'"
test-version snapshot cie-cassandra-3.0.19-custom 3.0.19.0-custom 3.0.19.0-custom-SNAPSHOT threezero three-zero-nineteen-zero-custom-snapshot
test-version snapshot cie-cassandra-3.0.19-custom 3.0.19.1-custom 3.0.19.1-custom-SNAPSHOT threezero three-zero-nineteen-one-custom-snapshot

test-version release  cie-cassandra-4.0.0 4.0.0.0 4.0.0.0           fourzero four-zero-zero-zero
test-version release  cie-cassandra-4.0.0 4.0.0.1 4.0.0.1           fourzero four-zero-zero-one
test-version snapshot cie-cassandra-4.0.0 4.0.0.0 4.0.0.0-SNAPSHOT  fourzero four-zero-zero-zero-snapshot
test-version snapshot cie-cassandra-4.0.0 4.0.0.1 4.0.0.1-SNAPSHOT  fourzero four-zero-zero-one-snapshot

test-fail release cie-cassandra-4.0.0-custom 4.0.0.0-custom "Only hotfix version sufix supported for release and this build has version suffix 'custom'"
test-fail release cie-cassandra-4.0.0-custom 4.0.0.1-custom "Only hotfix version sufix supported for release and this build has version suffix 'custom'"
test-version snapshot cie-cassandra-4.0.0-custom 4.0.0.0-custom 4.0.0.0-custom-SNAPSHOT fourzero four-zero-zero-zero-custom-snapshot
test-version snapshot cie-cassandra-4.0.0-custom 4.0.0.1-custom 4.0.0.1-custom-SNAPSHOT fourzero four-zero-zero-one-custom-snapshot

echo "All tests passed"
exit 0
