#!/usr/bin/env bash

#set -o xtrace
set -o errexit
set -o pipefail
set -o nounset

bin="$(cd "$(dirname "$0")" > /dev/null; pwd)"
home="$(cd "$(dirname "$bin")" > /dev/null; pwd)"

source "${bin}/functions.sh"

_run_tests() {
  local -r output_dir="$1"
  local -r kind="$2"
  local -r filter="${3:-}"
  local -r split_file="tests-${kind}.split"

  # cleanup any previous test run; but leave build as is to avoid recompiling
  rm -rf build/test || true

  # find all tests and create a large split
  bash -c "find 'test/$kind' -name '*Test.java' ${filter:-}" | sed "s;^test/$kind/;;g" > "$split_file"

  local java_version
  java_version=$(java -version 2>&1 | awk -F '"' '/version/ {print $2}' | awk -F. '{print $1}')
  if [ "$java_version" -ge 11 ]; then
    export CASSANDRA_USE_JDK11=true
  else
    unset CASSANDRA_USE_JDK11 || true
  fi

  local test_timeout
  test_timeout=$(grep "name=\"test.${kind}.timeout\"" build.xml | awk -F'"' '{print $4}' || true)
  if [ -z "$test_timeout" ]; then
    test_timeout=$(grep 'name="test.timeout"' build.xml | awk -F'"' '{print $4}')
  fi

  local rc=0
  ant testclasslist -Dtest.timeout="$test_timeout" -Dtest.classlistfile="$split_file"  -Dtest.classlistprefix="$kind" || rc=$?

  # copy test output into the output dir
  for d in output logs; do
    if [ -e build/test/"$d" ]; then
      mkdir -p "$output_dir/kind=$kind/build/test/$d"
      cp -r build/test/"$d"/* "$output_dir/kind=$kind/build/test/$d/"
    fi
  done
  return $rc
}

_run_unit() {
  _run_tests "$1" "unit"
}

_run_jvm_dtest() {
  _run_tests "$1" "distributed" "| grep -v upgrade"
}
_run_jvm_dtest_upgrade() {
  if [[ $(ls -1 build/dtest*.jar | wc -l) -lt 2 ]]; then
    echo 'skipping upgrade tests'
    return 0
  fi
  _run_tests "$1" "distributed" "| grep upgrade"
}

_main() {
  local -r output_dir="$1"

  cd "$home"

  _run_unit "$output_dir" || true
  _run_jvm_dtest "$output_dir" || true
  _run_jvm_dtest_upgrade "$output_dir" || true
}

_main "$@"
