#!/usr/bin/env bash

#set -o xtrace
set -o errexit
set -o pipefail
set -o nounset

bin="$(cd "$(dirname "$0")" > /dev/null; pwd)"

setup() {
  if ! type jq &> /dev/null; then
    yum install -y jq
  fi
  if ! type rio &> /dev/null; then
    git clone --quiet "https://github.pie.apple.com/dcapwell/ci-tools.git" /tmp/ci-tools
    export PATH="$PATH:/tmp/ci-tools"
  fi
}

_main() {
  setup

  rio trigger pie-oss-cassandra-build-cie-dtest-pr --repo-id pie-oss-cassandra-build \
    --param CASSANDRA_GIT_BRANCH "$GIT_PR_TARGET_BRANCH" \
    --param CASSANDRA_GIT_PR "$GIT_PR_ID" \
    --param CASSANDRA_GIT_URL "$GIT_URL" \
    --await
}

_main "$@"
