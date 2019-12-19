#!/usr/bin/env bash

##
## This script mimics rio but is targeted to run on a developers laptop.
##

#set -o xtrace
set -o errexit
set -o pipefail
set -o nounset

bin="$(cd "$(dirname "$0")" > /dev/null; pwd)"
home="$(cd "$(dirname "$bin")" > /dev/null; pwd)"

_abspath() {
  echo $(cd $(dirname "$1"); pwd)/$(basename "$1")
}

_main() {
  local readonly test_yaml="$1"

  local readonly user=parallelci
  local secret_name
  secret_name=$(kubectl get serviceaccounts "$user" -ojsonpath='{.secrets[0].name}' ; echo)
  local token
  token=$(kubectl get "secrets/$secret_name" -ojsonpath='{.data.token}' | base64 -D -)

  local mcqueen_yaml
  mcqueen_yaml=$(cat "$HOME/.parallel-ci/mcqueen/mcqueen.yml")

  # if output dir exists, delete it
  rm -rf $home/parallel-output || true

  docker pull docker.apple.com/piedb/parallelci:latest
  docker run -ti \
    -v $HOME/.m2:/root/.m2:ro \
    -v $home:$home \
    -w $home \
    docker.apple.com/piedb/parallelci:latest \
    bash -c "
#set -o xtrace
set -o errexit
set -o pipefail
set -o nounset

# mimic Rio Secrets
export BUILD_SECRETS_PATH=/tmp/dontlookhere
mkdir -p \"\$BUILD_SECRETS_PATH\"
echo -n '$token' > \"\$BUILD_SECRETS_PATH/k8s_auth_token\"
echo -n '$mcqueen_yaml' > \"\$BUILD_SECRETS_PATH/mcqueen.yml\"

# run the tests
'$bin/parallel-tests.sh' '$(_abspath $test_yaml)' '$home/parallel-output'
"
}

_main "$@"
