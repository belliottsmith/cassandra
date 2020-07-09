#! /bin/bash
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
        echo "Base "
        exit 1
    fi
}


function test-version {
    local -r buildtype="$1"
    local -r branchname="$2"
    local -r cieversion="$3"
    local -r expectbase="$4"
    local -r expectfull="$5"
    local -r expectstub="$6"
    local -r expectcvn="$7"
    local -r expectcs="$8"

    local -r base=$("$RIODIR/version-tool.sh" base "$buildtype" "$branchname" "$cieversion")
    local -r full=$("$RIODIR/version-tool.sh" full "$buildtype" "$branchname" "$cieversion")
    local -r stub=$("$RIODIR/stub-version.sh" "$full")
    local -r cvn=$("$RIODIR/carnival-compatible-name.sh" "$full")
    local -r cs=$("$RIODIR/version-tool.sh" carnivalsuffix "$buildtype" "$branchname" "$cieversion")


    expectresult base "$expectbase" "$base"
    expectresult full "$expectfull" "$full"
    expectresult stub "$expectstub" "$stub"
    expectresult carnival-version-name "$expectcvn" "$cvn"
    expectresult carnival-suffix "$expectcs" "$cs"

}

function test-fail {
    local -r buildtype="$1"
    local -r branchname="$2"
    local -r cieversion="$3"
    local -r expectbase="$4"


    if base=$("$RIODIR/version-tool.sh" base "$buildtype" "$branchname" "$cieversion" 2>/dev/null) &&
            full=$("$RIODIR/version-tool.sh" full "$buildtype" "$branchname" "$cieversion" 2>/dev/null) &&
            stub=$(./rio/stub-version.sh "$full" 2>/dev/null) &&
            cvn=$(./rio/carnival-compatible-name.sh "$full" 2>/dev/null)
    then
        echo "Expected failure for" "$@" "but no all version commands exited zero" >&2
        exit 1
    fi
}

test-version release cie-cassandra-3.0.19 3.0.19.0 3.0.19.0 3.0.19.0 threezero three-zero-nineteen-zero ""
test-version release cie-cassandra-3.0.19 3.0.19.1 3.0.19.1 3.0.19.1 threezero three-zero-nineteen-one ""
test-version snapshot cie-cassandra-3.0.19 3.0.19.0 3.0.19 3.0.19-next threezero three-zero-nineteen-next "-snapshot"
test-version snapshot cie-cassandra-3.0.19 3.0.19.1 3.0.19 3.0.19-next threezero three-zero-nineteen-next "-snapshot"

test-version release cie-cassandra-3.0.19.0-hotfix 3.0.19.0-hotfix 3.0.19.0 3.0.19.0 threezero three-zero-nineteen-zero "-hotfix"
test-version release cie-cassandra-3.0.19.0-hotfix 3.0.19.1-hotfix 3.0.19.1 3.0.19.1 threezero three-zero-nineteen-one "-hotfix"
test-version snapshot cie-cassandra-3.0.19.0-hotfix 3.0.19.0-hotfix 3.0.19 3.0.19-hotfix threezero three-zero-nineteen-hotfix "-hotfixsnapshot"
test-version snapshot cie-cassandra-3.0.19.0-hotfix 3.0.19.1-hotfix 3.0.19 3.0.19-hotfix threezero three-zero-nineteen-hotfix "-hotfixsnapshot"

test-fail release cie-cassandra-3.0.19-custom 3.0.19.0-custom
test-fail release cie-cassandra-3.0.19-custom 3.0.19.1-custom
test-version snapshot cie-cassandra-3.0.19-custom 3.0.19.0-custom 3.0.19 3.0.19-custom threezero three-zero-nineteen-custom "-customsnapshot"
test-version snapshot cie-cassandra-3.0.19-custom 3.0.19.1-custom 3.0.19 3.0.19-custom threezero three-zero-nineteen-custom "-customsnapshot"

test-version release cie-cassandra-4.0.0 4.0.0.0 4.0.0.0 4.0.0.0 fourzero four-zero-zero-zero ""
test-version release cie-cassandra-4.0.0 4.0.0.1 4.0.0.1 4.0.0.1 fourzero four-zero-zero-one  ""
test-version snapshot cie-cassandra-4.0.0 4.0.0.0 4.0.0 4.0.0-next fourzero four-zero-zero-next "-snapshot"
test-version snapshot cie-cassandra-4.0.0 4.0.0.1 4.0.0 4.0.0-next fourzero four-zero-zero-next "-snapshot"

test-fail release cie-cassandra-4.0.0-custom 4.0.0.0-custom
test-fail release cie-cassandra-4.0.0-custom 4.0.0.1-custom
test-version snapshot cie-cassandra-4.0.0-custom 4.0.0.0-custom 4.0.0 4.0.0-custom fourzero four-zero-zero-custom "-customsnapshot"
test-version snapshot cie-cassandra-4.0.0-custom 4.0.0.1-custom 4.0.0 4.0.0-custom fourzero four-zero-zero-custom "-customsnapshot"

echo "All tests passed"
exit 0
