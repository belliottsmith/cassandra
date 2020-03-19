#!/usr/bin/env bash

##
## This script is meant to run in CI as a means to run all the unit tests in parallel.  This uses a tool called parallel-ci so requires that this is setup first before calling this script.
##

#set -o xtrace
set -o errexit
set -o pipefail
set -o nounset

bin="$(cd "$(dirname "$0")" > /dev/null; pwd)"
home="$(cd "$(dirname "$bin")" > /dev/null; pwd)"

namespace=cassandra-storage-test
user=parallelci
cluster=usprz1
server=https://api.usprz1.applecloud.io:443

_abspath() {
  echo $(cd $(dirname "$1"); pwd)/$(basename "$1")
}

_setup_k8s() {
  # update k8s config to include the user with the service account token
  local token
  token=$(cat $BUILD_SECRETS_PATH/k8s_auth_token)
  kubectl config set-credentials "$user" --token="$token"

  # define the cluster to use
  kubectl config set-cluster "$cluster" --server="$server" --insecure-skip-tls-verify=true

  # create a new context which links the user with the cluster
  kubectl config set-context "$cluster-with-sa" --cluster="$cluster" --user="$user" --namespace="$namespace"

  # switch to the new context
  kubectl config use-context "$cluster-with-sa"
}

_setup_parallelci() {
  mkdir -p "$HOME/.parallel-ci/mcqueen"
  cp "$BUILD_SECRETS_PATH/mcqueen.yml" "$HOME/.parallel-ci/mcqueen/mcqueen.yml"
}

_extract() {
  local readonly output="$1"

  # extract the results
  # Rio doesn't have a way to segregate output (Jenkins does!) so this script uses the testsuite name attribute to group based off the directory structure (eg. unit.java=8).
  mkdir -p "$home/build" || true
  local dir
  local name
  local untar_name
  local suite_name
  local out_dir
  for map in $output/*/*/map-*; do 
    dir="$(dirname "$map")"
    name="$(basename "$map")"
    untar_name="${name%.tar.gz}"
    suite_name="$( echo "${dir#$output/}" | tr '/' '.' )"
    out_dir="$dir/$untar_name"

    mkdir "$out_dir"
    tar zxf "$map" -C "$out_dir"
    rm "$map"
    for xml in $( find "$out_dir" -name "TEST-*.xml" ); do
      "$bin/replace-suite-name.py" "$xml" "$suite_name"
      # to avoid conflict, prefix the file name with the suite_name
      mv "$xml" "$(dirname "$xml")/$suite_name-$(basename "$xml")"
    done
  done
  # copy the contents from parallel output to ant's build dir
  # 3 is for: unit/java=?/[task]
  cp -r $output/*/*/*/build/* "$home/build/"
}

_main() {
  local -r yaml="$1"
  local output="$2"
  output="$( _abspath "$output" )"

  _setup_k8s
  _setup_parallelci

  labels=(
    "rio"
    "rio-build=$PIPELINE_SPEC_ID"
    "rio-build-number=$RIO_BUILD_NUMBER"
  )
  parallelci "$home" "$yaml" "$output" "${labels[@]}"
  _extract "$output"
}

_main "$@"
