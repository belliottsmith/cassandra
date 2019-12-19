#! /bin/bash

set -o nounset
set -o errexit
set -o pipefail

readonly RIO_DIR="$(dirname "$0")"
readonly BASE_DIR="${RIO_DIR}/.."
readonly buildtype="${1:-}"

case $buildtype in
    "snapshot")
        readonly SKIP_FILE="${BASE_DIR}/CIE-SKIP-SNAPSHOT-TESTS"
        readonly OVERRIDE_FILE="${BASE_DIR}/CIE-OVERIDE-SNAPSHOT-TEST-FAILURES"
        ;;
    "release")
        readonly SKIP_FILE="/you/may/never/ever/skip/tests/on/release/only/ignore/if/you/have/very/good/reasons"
        readonly OVERRIDE_FILE="${BASE_DIR}/CIE-OVERIDE-RELEASE-TEST-FAILURES"
        ;;
    *)
        echo "Must specify snapshot or release build to determine skip files" >&2
        exit 1
        ;;
esac


cleanup() {
    if [ -d "${BASE_DIR}/build/test/output" ]; then
        echo "Copying ParallelCI output for Rio to pass on to Quality"
        mkdir -p "${BASE_DIR}/.out/test-results"
        find "${BASE_DIR}/build/test/output" -name '*.xml' | while read -r xml
        do
            cp "$xml" .out/test-results/
        done
        find .out/test-results/ -ls
    fi
}

trap cleanup EXIT
trap cleanup ERR

"$RIO_DIR/parallel-tests.sh" ./rio/unittests.yml parallel-output
