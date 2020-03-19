#! /bin/bash

set -o nounset
set -o errexit
set -o pipefail
set -o xtrace

readonly RIO_DIR="$(dirname "$0")"
readonly BASE_DIR="${RIO_DIR}/.."

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

# Extract the count of errors and failures from the junit reports.
# ParallelCI will prefix the test XML names with the jvm scope used,
# then extract the summary testsuite lines and use bash arithmetic
# to compute a total of errors and failures.
ERRFAILS="$(( $(find "${BASE_DIR}/build/test/output" -name '*TEST-*.xml' -print0 | \
                xargs -0 grep '^<testsuite' | \
                sed -e 's/^.*errors="\([0-9]*\)" failures="\([0-9]*\)".*$/+\1+\2/g') ))"
if [ $((ERRFAILS > 0)) ]
then
    echo "### " >&2
    echo "### FAILED - $ERRFAILS failures found in junittest reports" >&2
    echo "### " >&2
    exit 1
else
    echo "### Tests passed - $ERRFAILS errors/failures detected"
    exit 0
fi
