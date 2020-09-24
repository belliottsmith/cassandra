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

KUBE_CLUSTER="${KUBE_CLUSTER:-us-west-1a}"
if [[ -e "$BUILD_SECRETS_PATH/k8s_auth_token.$KUBE_CLUSTER" ]]; then
  token_file="$BUILD_SECRETS_PATH/k8s_auth_token.$KUBE_CLUSTER"
else
  token_file="$BUILD_SECRETS_PATH/k8s_auth_token"
fi

user=parallelci
case "$KUBE_CLUSTER" in
   us-west-1a)
     namespace=aci-cassandra
     server=https://kube-api.us-west-1a.aci.apple.com:443
   ;;
   usprz1)
     namespace=cassandra-storage-test
     server=https://api.usprz1.applecloud.io:443
   ;;
esac

_is_apc() {
  local -r cluster="$1"
  case "$cluster" in
    usprz1)
      return 0
    ;;
    *)
      return 1
    ;;
  esac
}

_abspath() {
  echo "$(cd "$(dirname "$1")"; pwd)/$(basename "$1")"
}

_setup_k8s() {
  # update k8s config to include the user with the service account token
  local token
  echo "Using token file $token_file"
  token=$(cat "$token_file")
  kubectl config set-credentials "$user" --token="$token"

  # define the cluster to use
  kubectl config set-cluster "$KUBE_CLUSTER" --server="$server" --insecure-skip-tls-verify=true

  # create a new context which links the user with the cluster
  kubectl config set-context "${KUBE_CLUSTER}-with-sa" --cluster="$KUBE_CLUSTER" --user="$user" --namespace="$namespace"

  # switch to the new context
  kubectl config use-context "${KUBE_CLUSTER}-with-sa"
}

_setup_parallelci() {
  mkdir -p "$HOME/.parallel-ci/mcqueen"
  cp "$BUILD_SECRETS_PATH/mcqueen.yml" "$HOME/.parallel-ci/mcqueen/mcqueen.yml"
}

_extract() {
  local -r output="$1"

  # extract the results
  # Rio doesn't have a way to segregate output (Jenkins does!) so this script uses the testsuite name attribute to group based off the directory structure (eg. unit.java=8).
  mkdir -p "$home/build" || true
  local dir
  local name
  local untar_name
  local suite_name
  local out_dir
  for map in "$output"/*/*/map-*; do
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
}

# Labels must be 63 chars or less and start with an alphanumeric char
_trim_label() {
  local -r label="$1"
  local -r candidate="${label:${#label}<63?0:-63}"
  case "${candidate}" in
    [a-z]* | [A-Z]* | [0-9]*)
        echo "${candidate}"
        ;;
    "")
        echo no-label
        ;;
    *)
        _trim_label "${candidate: 1-$((${#candidate}-1))}"
        ;;
  esac
}

# clone the git branch only
git_clone() {
  local -r url="$1"
  local -r branch="$2"
  local -r out_dir="$3"

  git clone --depth 1 "$url" -b "$branch" "$out_dir"
  (cd "$out_dir"; echo "Cloned $url $branch to $out_dir; commit is $( git rev-parse HEAD )")
}

# apply any changes needed to get Apache Cassandra to build in Apple
apply_cassandra_patches() {
  local -r dir="$1"
  local -r patch_dir="$bin/patches/cassandra"

  cp "$patch_dir/build.properties.default" "$dir/build.properties.default"
  cp "$patch_dir/parallelciignore" "$dir/.parallelciignore"
}

# clone cassandra and apply patches
clone_cassandra() {
  local -r url="$1"
  local -r branch="$2"
  local -r out_dir="$3"

  git_clone "$url" "$branch" "$out_dir"

  apply_cassandra_patches "$out_dir"
}

_download_and_build() {
  local -r version="$1"
  local -r output_dir="$2"

  # prefix stdout and stderr with the branch being built so its clear where logs are coming from
  exec > >( awk "{ print \"dtest-$version.jar> \", \$0 } " )
  exec 2> >( awk "{ print \"dtest-$version.jar> \", \$0 } " )

  # why hard code github?  If a Apple fork is used, the fork may be very out of date with the
  # different branches which could lead to failing tests caused by out dated dtest jars to avoid
  # this, rely on github
  clone_cassandra "${CASSANDRA_UPGRADE_GIT_URL:-https://github.com/apache/cassandra.git}" "$version" "/tmp/$version"
  cd "/tmp/$version"
  # make sure to build jars outside of parallel ci since network access is more limited
  ant dtest-jar
  cp build/dtest*.jar "$output_dir"
}

_parallel_clone_branches() {
  local -r output_dir="$1"; shift

  mkdir -p "$output_dir"
  pids=()
  for version in "$@"; do
    _download_and_build "$version" "$output_dir" &
    pids+=( $! )
  done
  for pid in "${pids[@]}"; do
    wait $pid
  done
}

_main() {
  local yaml="$1"
  local output="$2"
  output="$( _abspath "$output" )"

  if _is_apc "$KUBE_CLUSTER" ; then
    # the resources should target kube, so need to downgrade to apc resources
    cat "$yaml" | awk -f "$bin/apc_downgrade.awk" > "$yaml.$KUBE_CLUSTER"
    echo "Downgrading resources to match APC; this assumes resources were tuned for Kube"
    diff "$yaml" "$yaml.$KUBE_CLUSTER" || true
    yaml="$yaml.$KUBE_CLUSTER"
  fi

  _setup_k8s
  _setup_parallelci

  if [ ! -z "${CASSANDRA_DTEST_VERSIONS:-}" ]; then
    # In order to run jvm dtest upgrade tests, check out all versions requested
    # CASSANDRA_DTEST_VERSIONS is not quoted that way each space acts as a different argument to the function
    # make sure to call this AFTER cloning cassandra, else will need to copy jars here anyways
    _parallel_clone_branches "$(abspath dtest_jars)" $CASSANDRA_DTEST_VERSIONS
  fi

  # Labels are limited to 63 characters... pipeline spec id can exceed that
  labels=(
    "rio"
    "rio-build=$(_trim_label "$PIPELINE_SPEC_ID")"
    "rio-build-number=$RIO_BUILD_NUMBER"
  )
  parallelci "$home" "$yaml" "$output" "${labels[@]}"
  _extract "$output"

  # Move the log4j files generated by tests into a unified output
  # directory for a better user experience in Jenkins.  Only move
  # log files so the output from parallelci is preserved to be
  # collected by mapper.
  find parallel-output -name '*.log' -print0 | xargs -0 "$bin/unify-files.sh" parallel-output unified-output
}

_main "$@"
