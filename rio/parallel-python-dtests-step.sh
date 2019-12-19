#!/usr/bin/env bash

set -o xtrace
set -o errexit
set -o pipefail
set -o nounset

bin="$(cd "$(dirname "$0")" > /dev/null; pwd)"

source "$bin/functions.sh"

readonly CI_NAME=".ci_src"
readonly CI_DIR="$PWD/$CI_NAME"

_main() {
  export CI_CONTEXT_CASSANDRA_DIR="$PWD"
  export DEBUG=true
  export DTEST_GIT_BRANCH="cie"
  export DTEST_GIT_URL="git@github.pie.apple.com:aci/apache-cassandra-dtest.git"
  export KUBE_CLUSTER="us-west-1a"
  export MIGRATED_OFF_LOG4J2=true
  export PARALLELCI_DTEST_YAML="cie-dtests.yml"

  mkdir context || true
  if [[ -e .parallelciignore ]]; then
    while read -r line; do
      if [[ "$line" == "build" ]]; then
        # unit tests don't want the build dir in the context, as they compile in the container (and
        # in rio to detect compile issues early); this means python-dtest can't start a cluster
        # since its not built!
        # Add a include pattern to make sure everything in the build directory gets added
        echo "!cassandra/build/**" >> context/.parallelciignore
      elif [[ "$line" == '**/'* ]]; then
        # if already a glob, do not add
        echo "$line" >> context/.parallelciignore
      else
        # add cassandra prefix so the match respects new root
        # current patterns are relative to '.', but this got moved to context/cassandra where '.'
        # is now 'context', so need 'cassandra/' prefix to match
        echo "cassandra/$line" >> context/.parallelciignore
      fi
    done < .parallelciignore
  fi
  # common exclude logic
  for n in "cassandra/context" "cassandra/$CI_NAME" "**/.git"; do
    echo "$n" >> context/.parallelciignore
  done

  git_clone "git@github.pie.apple.com:pie/oss-cassandra-build.git" "master" "$CI_DIR"
  "$CI_DIR"/ci/dtest.sh
}

_main "$@"
