#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

##
## Superproject commits are only buildable if the submodule commits they record can be fetched, so before publishing
## superproject commits verify that every submodule commit they record is on a remote-tracking branch of a network
## remote of the submodule (remotes on the local filesystem do not count, as others cannot fetch from them).
##
## To skip the check for one push, use CASSANDRA_SKIP_SUBMODULE_PUSH_CHECK=1 git push ..., or git push --no-verify
## (which also skips any other pre-push hooks).
##
## Configuration:
##   git config cassandra.submodules.verify-push false    # disable this check
##   git config cassandra.submodules.push on-demand       # push missing submodule commits, rather than failing:
##                                                        # the submodule branch named as the superproject branch being
##                                                        # pushed (else the submodule's current branch) is pushed to the
##                                                        # submodule remote named as the superproject remote (else its
##                                                        # only network remote, or cassandra.submodules.push-remote)
##

# Redirect output to stderr.
exec 1>&2

#set -o xtrace
set -o errexit
set -o pipefail
set -o nounset

cd "$(git rev-parse --show-toplevel)"
source "$(dirname "$0")/../cassandra-lib/submodules.sh"

error() {
  _log "$@"
  exit 1
}

# whether submodule commit $2 is on a remote-tracking branch of a network remote of submodule $1
is_published() {
  local -r path="$1" sha="$2"
  local remote
  subgit "$path" cat-file -e "$sha^{commit}" 2> /dev/null || return 1
  for remote in $(network_remotes "$path"); do
    [[ -n "$(subgit "$path" for-each-ref --count=1 --contains "$sha" "refs/remotes/$remote/")" ]] && return 0
  done
  return 1
}

# the submodule remote to push to on demand
push_remote() {
  local -r path="$1" super_remote="$2"
  local remote
  remote="$(git config cassandra.submodules.push-remote || true)"
  [[ -n "$remote" ]] && { echo "$remote"; return 0; }
  if subgit "$path" remote get-url "$super_remote" &> /dev/null && is_network_url "$(subgit "$path" remote get-url "$super_remote")"; then
    echo "$super_remote"
    return 0
  fi
  local -r remotes="$(network_remotes "$path")"
  [[ -n "$remotes" && "$(echo "$remotes" | wc -l)" -eq 1 ]] && echo "$remotes"
  return 0
}

# push the missing commits $3... of submodule $1 on demand, for a push to superproject remote $2
push_on_demand() {
  local -r path="$1" super_remote="$2"
  shift 2
  local remote branch candidate sha contains_all lease=""
  remote="$(push_remote "$path" "$super_remote")"
  [[ -z "$remote" ]] && { _log "$path: cannot choose a network remote to push to; set cassandra.submodules.push-remote"; return 1; }
  is_network_url "$(subgit "$path" remote get-url "$remote")" || { _log "$path: remote $remote is on the local filesystem"; return 1; }

  # prefer the submodule branch named as a superproject branch being pushed, else the submodule's current branch
  branch=""
  for candidate in ${pushed_branches[@]+"${pushed_branches[@]}"} "$(subgit "$path" symbolic-ref -q --short HEAD || true)"; do
    [[ -z "$candidate" ]] && continue
    subgit "$path" rev-parse -q --verify "refs/heads/$candidate" > /dev/null || continue
    contains_all=true
    for sha in "$@"; do
      subgit "$path" merge-base --is-ancestor "$sha" "refs/heads/$candidate" 2> /dev/null || { contains_all=false; break; }
    done
    if $contains_all; then
      branch="$candidate"
      break
    fi
  done
  [[ -z "$branch" ]] && { _log "$path: no branch contains every commit to push"; return 1; }

  # a rewritten submodule branch may only be forced if the superproject push is itself forced
  local -r tracking="$(subgit "$path" rev-parse -q --verify "refs/remotes/$remote/$branch" || true)"
  if [[ -n "$tracking" ]] && ! subgit "$path" merge-base --is-ancestor "$tracking" "refs/heads/$branch" 2> /dev/null; then
    $forced || { _log "$path: $remote/$branch has been rewritten, but this push is not forced"; return 1; }
    lease="--force-with-lease=refs/heads/$branch:$tracking"
  fi
  _log "$path: pushing $branch to $remote${lease:+ (forced)}"
  subgit "$path" push ${lease:+"$lease"} "$remote" "refs/heads/$branch:refs/heads/$branch"
}

[[ -n "${CASSANDRA_SKIP_SUBMODULE_PUSH_CHECK:-}" ]] && exit 0
[[ "$(git config --bool cassandra.submodules.verify-push || echo true)" == false ]] && exit 0
# honour the configuration of the pre-commit hook this replaces
[[ "$(git config --bool cassandra.pre-commit.verify-submodules.enabled || echo true)" == false ]] && exit 0
mode="$(git config cassandra.submodules.push || echo check)"
[[ "$mode" == check || "$mode" == on-demand ]] || error "cassandra.submodules.push must be check or on-demand, not $mode"

super_remote="$1"
paths=( $(submodule_paths) )
[[ ${#paths[@]} -eq 0 ]] && exit 0

# collect the superproject commits being pushed that may record a new submodule commit: those changing a submodule
# (submodule commits recorded by commits already on a remote were verified when they were pushed), and each pushed tip
pushed_branches=()
forced=false
commits="$(mktemp)"
trap 'rm -f "$commits"' EXIT
while read -r local_ref local_sha remote_ref remote_sha; do
  [[ -z "${local_ref:-}" ]] && continue
  is_null_sha "$local_sha" && continue # deletion
  [[ "$local_ref" == refs/heads/* ]] && pushed_branches+=("${local_ref#refs/heads/}")
  if ! is_null_sha "$remote_sha" && git cat-file -e "$remote_sha^{commit}" 2> /dev/null; then
    git merge-base --is-ancestor "$remote_sha" "$local_sha" || forced=true
    git rev-list "$local_sha" "^$remote_sha" -- "${paths[@]}" >> "$commits"
  else
    is_null_sha "$remote_sha" || forced=true # an unknown remote commit: we must be overwriting it
    git rev-list "$local_sha" --not --remotes -- "${paths[@]}" >> "$commits"
  fi
  echo "$local_sha" >> "$commits"
done

# the submodule commits recorded, with a superproject commit recording each
missing=()
missing_by=()
for path in "${paths[@]}"; do
  if [[ ! -e "$path/.git" ]]; then
    _log "$path is not checked out, so the submodule commits pushed cannot be verified"
    continue
  fi
  fetched=false
  for sha_and_commit in $(sort -u "$commits" | while read -r commit; do
                            sha="$(gitlink "$commit" "$path")"
                            [[ -n "$sha" ]] && echo "$sha:$commit"
                          done | sort -t: -k1,1 -u); do
    sha="${sha_and_commit%%:*}"
    is_published "$path" "$sha" && continue
    if ! $fetched && [[ -z "${BUILD_OFFLINE:-}" ]]; then
      # we may have pushed from elsewhere since we last fetched
      for remote in $(network_remotes "$path"); do
        subgit "$path" fetch --quiet "$remote" || _log "$path: could not fetch $remote"
      done
      fetched=true
      is_published "$path" "$sha" && continue
    fi
    missing+=("$path:$sha")
    missing_by+=("${sha_and_commit#*:}")
  done
done
[[ ${#missing[@]} -eq 0 ]] && exit 0

if [[ "$mode" == on-demand ]]; then
  for path in "${paths[@]}"; do
    shas=()
    for entry in "${missing[@]}"; do
      [[ "${entry%:*}" == "$path" ]] && shas+=("${entry##*:}")
    done
    [[ ${#shas[@]} -eq 0 ]] && continue
    push_on_demand "$path" "$super_remote" "${shas[@]}" || continue
    remaining=()
    remaining_by=()
    for i in "${!missing[@]}"; do
      entry="${missing[$i]}"
      if [[ "${entry%:*}" != "$path" ]] || ! is_published "$path" "${entry##*:}"; then
        remaining+=("$entry")
        remaining_by+=("${missing_by[$i]}")
      fi
    done
    missing=( ${remaining[@]+"${remaining[@]}"} )
    missing_by=( ${remaining_by[@]+"${remaining_by[@]}"} )
  done
  [[ ${#missing[@]} -eq 0 ]] && exit 0
fi

_log "Refusing to push superproject commits recording submodule commits not found on a network remote of the submodule:"
for i in "${!missing[@]}"; do
  entry="${missing[$i]}"
  _log "\t${entry%:*} ${entry##*:} (recorded by $(git log -1 --format='%h "%s"' "${missing_by[$i]}"))"
done
_log "Remotes on the local filesystem do not count, as others cannot fetch from them. To fix, either"
_log "\tpush the submodule commits, e.g.: git -C ${missing[0]%:*} push <remote> <branch>"
_log "\tpush them automatically on demand: git config cassandra.submodules.push on-demand"
_log "\tor skip this check: CASSANDRA_SKIP_SUBMODULE_PUSH_CHECK=1 git push ... (or git push --no-verify)"
exit 1
