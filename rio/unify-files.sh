#! /usr/bin/env bash
#
# Helper script to be called by xargs to move test log files
# from the parallel test output directories to a unified output
# directory.  Jenkins (as used by Rio) can collect logs and
# without this the log files are listed by the mapper and hard
# to find the right one.
#
# find parallel-output -name '*.log' -print0 | xargs -0 unify-files parallel-output unified-output
set -o nounset
set -o errexit
set -o pipefail

readonly paralleloutput="$1"
readonly unifiedoutput="$2"
shift 2

function make_unified_path() {
    echo "$1" | sed -e "s!^${paralleloutput}!${unifiedoutput}!g" -e 's!map-[0-9][0-9]*-output/!!g'
}

for parallelpath in "$@"
do
    unifiedpath="$(make_unified_path "$parallelpath")"
    mkdir -p "$(dirname "$unifiedpath")"
    mv "$parallelpath" "$unifiedpath"
done
