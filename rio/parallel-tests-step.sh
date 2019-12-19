#!/usr/bin/env bash

set -o nounset
set -o errexit
set -o pipefail
set -o xtrace

readonly RIO_DIR="$(dirname "$0")"
readonly BASE_DIR="$(cd "$RIO_DIR/.."; pwd)"
readonly PARALLELOUTPUT="${BASE_DIR}/parallel-output"

target="${1:-unit}"

source "${RIO_DIR}/functions.sh"

# build locally so parallel ci copies the jars into the container
rm -rf "${BASE_DIR}/build" || true
timeout 15m bash -c "cd '$BASE_DIR' && ant -f rio-build.xml jar "

# Wrap call to parallel-tests with timeout as per-command
# timeouts are not currently implemented and this gives us
# a chance of having Rio examine any created output
#
if [[ -e "${BASE_DIR}/disable-parallel-tests" ]]; then
  timeout 160m "$RIO_DIR/sequential-tests.sh" "$PARALLELOUTPUT"
else
  args=(
    ./rio/unittests.yml
    "$PARALLELOUTPUT"
  )
  if [[ "$target" == "unit" ]]; then
    args+=( --exclude-work jvm-dtest --exclude-work jvm-dtest-upgrade)
  elif [[ "$target" == "jvm-dtest" ]]; then
    args+=( --exclude-work unit --exclude-work jvm-dtest-upgrade)
  elif [[ "$target" == "jvm-dtest-upgrade" ]]; then
    args+=( --exclude-work unit --exclude-work jvm-dtest)
  fi
  timeout 160m "$RIO_DIR/parallel-tests.sh" "${args[@]}"
fi

# Extract the count of errors and failures from the junit reports.
# ParallelCI will prefix the test XML names with the jvm scope used,
# then extract the summary testsuite lines and use bash arithmetic
# to compute a total of errors and failures.
ERRFAILS="$(( $(find "${PARALLELOUTPUT}" -name '*TEST-*.xml' -print0 | \
                xargs -0 grep '^<testsuite' | \
                sed -e 's/^.*errors="\([0-9]*\)" failures="\([0-9]*\)".*$/+\1+\2/g') ))"
if [ "$ERRFAILS" -gt 0 ]
then
    echo "### " >&2
    echo "### FAILED - $ERRFAILS failures found in junittest reports" >&2
    echo "### " >&2
    exit 1
else
    echo "### Tests passed - $ERRFAILS errors/failures detected"
fi

_verify_command_success "$PARALLELOUTPUT"
