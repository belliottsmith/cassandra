#!/usr/bin/env bash

#set -o xtrace
set -o errexit
set -o pipefail
set -o nounset

request() {
  local -r url="$1"; shift
  local -r request_id="$(uuidgen)"
  local -r output_file="/tmp/requests/$request_id"

  mkdir -p "$(dirname "$output_file")"

  local status_code
  while true; do
    status_code=$(curl -s --show-error -o "$output_file" -w "%{http_code}" "$@" "$url")
    if [[ $status_code -ge 500 ]]; then
      # server error, try again
      true
    elif [[ $status_code -ge 400 ]]; then
      # client error, fail
      cat "$output_file" >&2
      return $status_code
    else
      # all good
      cat "$output_file"
      return 0
    fi
  done
}

trigger_rio_pipeline() {
  local -r _repo_id="$1"
  local -r _spec_id="$2"
  local -r _build_parameters="$3"

  read rio_api_token < ${BUILD_SECRETS_PATH}/rio-api-token || :
  read rio_email < ${BUILD_SECRETS_PATH}/rio-test-user-email || :

  local response
  response=$(request https://rio-api.pie.apple.com/v1/projects/${_repo_id}/pipeline_specs/${_spec_id}/trigger \
    -H "X-RIO-API-EMAIL: ${rio_email}" \
    -H "X-RIO-API-TOKEN: ${rio_api_token}" \
    -H "Content-Type: application/json" \
    -d '{"build_params":{'"${_build_parameters}"'}}')

  local pipeline_id
  pipeline_id=$(echo "${response}" | jq -r .pipelineId)

  # Wait for the pipeline to complete.
  local state
  while true; do
    response=$(request https://rio-api.pie.apple.com/v1/projects/${_repo_id}/pipelines/${pipeline_id} \
        -H "X-RIO-API-EMAIL: ${rio_email}"                                  \
        -H "X-RIO-API-TOKEN: ${rio_api_token}"                              \
        -H "Content-Type: application/json")
    state=$(echo $response | jq -r .status)
    case ${state} in
        complete)
            echo "note: pipeline complete" 1>&2
            result=$(echo $response | jq -r .result)
            if [ "${result}" == "success" ]
            then
              echo "Harry build has succeeded"
            else
              echo "Harry build did not succeed with ${result}; see https://rio.apple.com/projects/${_repo_id}/pipeline-specs/${_spec_id}/pipelines/${pipeline_id}"
              exit 1
            fi
            ;;
        started|in-progress)
            echo "note: pipeline $state, waiting for pipeline to complete..." 1>&2
            sleep 1
            continue
            ;;
        *)
            echo "error: unexpected pipeline state: ${state}" 1>&2
            exit 1
      esac
    break
  done
}

info() {
  echo "$*"
}
_main() {
    if ! type jq &> /dev/null; then
      yum install -y jq
    fi

    # trigger harry build
    build_params=(
      "\"CASSANDRA_REPOSITORY\":\"aci-cassandra/aci-cassandra\"" ,
      "\"CASSANDRA_PR\":\"$GIT_PR_ID\"" ,
      "\"CASSANDRA_BRANCH\":\"$GIT_BRANCH\"" ,
      "\"HARRY_BRANCH\":\"master\"" ,
      "\"DURATION_IN_MINUTES\":\"15\"" ,
      "\"PARALLELISM\":\"2\"" ,
      "\"COMPLETIONS\":\"2\""
    )
    echo "Triggering rio build pie-harry-build-cassandra-and-run-build with build params ${build_params[*]}"
    trigger_rio_pipeline pie-harry pie-harry-build-cassandra-and-run-build "${build_params[*]}"
}

_main "$@"
