#!/usr/bin/env bash

#set -o xtrace
set -o errexit
set -o pipefail
set -o nounset

bin="$(cd "$(dirname "$0")" > /dev/null; pwd)"
home="$(cd $bin/.. > /dev/null; pwd)"

_main() {
  local -r user=parallelci
  local -r real_user="$USER"
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
    -v $HOME/.m2:$HOME/.m2:ro \
    -v $home:$home \
    -w $home \
    docker.apple.com/piedb/parallelci:latest \
    bash -c "
# mimic Rio Secrets
export BUILD_SECRETS_PATH=/tmp/dontlookhere
mkdir -p \"\$BUILD_SECRETS_PATH\"
echo -n '$token' > \"\$BUILD_SECRETS_PATH/k8s_auth_token\"
echo -n '$mcqueen_yaml' > \"\$BUILD_SECRETS_PATH/mcqueen.yml\"

# mimic Rio build environments
export PIPELINE_SPEC_ID="${real_user}-rio-local"
export RIO_BUILD_NUMBER=42
# this is a flat out lie, but needed to make sure you can test the build train
export GIT_BRANCH=cie-cassandra-3.0.19

bash
"
}

_main "$@"
