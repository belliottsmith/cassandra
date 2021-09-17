#!/usr/bin/env bash
#
# Return the RELEASE_VERSION used in the rio.yml configuration.
#
# Usage:
#  version-tool {snapshot | release}
#
# To calculate versions
# 1) Extract the version information (x.y.z) from the branchname of the form cie-cassandra-x.y.z
# 2) Read the CIE-VERSION file for the last released version (p.q.r.s) and check the branch version is a prefix
#
# For snapshots, return the last three dotted integers
# For releases, return all four dotted integers and check the current HEAD is tagged cie-cassandra-p.q.r.s
#
# Unit tests in rio/version-tool-test.sh
#
set -o errexit
set -o pipefail
set -o nounset

readonly buildtype="${1:-}"
readonly branchname="${2:-}"
readonly cieversion="${3:-}"

IFS='-' read -r filedotted filesuffix <<< "$cieversion"
IFS='.' read -ra fileelements <<< "$filedotted"

function array_pop { echo "${@:1:(($#-1))}"; }
function join { local IFS="$1"; shift; echo "$*"; }

# Parse the branch name; assume cie-cassandra-{major}.{minor}.{patch}(.{hotfix})?(-{tag})?
case "$branchname" in
    cie-cassandra-*)
        IFS='-' read -r branchdotted branchsuffix <<< "${branchname#cie-cassandra-}"
        ;;
    *)
        echo "================================================================"
        echo "Not a standard build branch - if this is your own fork, either"
        echo "update RELEASE_VERSION in rio.yml, or update rio/version-tool.sh "
        echo "to support your branching scheme)"
        exit 1
        ;;
esac

# Make sure that the branch and the CIE-VERSION "match".  Since a branch can produce multiple
# releases it is assumed that the branch name is the {major}.{minor}.{patch} format and that
# CIE-VERSION is {major}.{minor}.{patch}.{hotfix} format, so check that removing the {hotfix}
# matches the branch, OR the file suffix is "hotfix"; add "-hotfix" to the end of CIE-VERSION
# to ignore this check.
readonly fileprefix="$(join . $(array_pop "${fileelements[@]}"))"
if [ "$fileprefix" != "$branchdotted" ] && [ "$filesuffix" != "hotfix" ]
then
    echo "Branch name version must be a prefix of version in the root level CIE-VERSION file"
    echo "File version prefix '${fileprefix}' branch '${branchdotted}'"
    exit 1
fi

# If the branch has a suffix, it should also match in CIE-VERSION.
# example:
# branch: cie-cassandra-3.0.19.0-hotfix
# CIE-VERSION: 3.0.19.21-hotfix
if [ "$branchsuffix" != "$filesuffix" ]
then
    echo "Version suffix must match branch name and CIE-VERSION file"
    exit 1
fi


# Generate the version string, this is based off the build type
case "${buildtype}" in
    "snapshot")
        echo "$cieversion-SNAPSHOT"
        ;;
    "release")
        # If file elements are not equal four, if filesuffix is not empty or hotfix, no release
        if [ ${#fileelements[@]} -ne 4 ]
        then
            echo "================================================================" >&2
            echo "Expected 4 elements in a CIE release version number and no version suffix '${filedotted}' has ${#fileelements[@]}" >&2
            exit 1
        fi
        if [ "${filesuffix:-}" != "" ] && [ "${filesuffix:-}" != "hotfix" ]
        then
            echo "================================================================" >&2
            echo "Only hotfix version sufix supported for release and this build has version suffix '${filesuffix:-}'" >&2
            exit 1
        fi
        join . "${fileelements[@]}"
        ;;
    *)
        echo "Must request a build type of {snapshot,release}"
        exit 1
esac
