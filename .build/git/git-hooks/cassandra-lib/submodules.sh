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
## Functions shared by the submodule hooks; sourced, not executed.
##

_log() {
  echo -e "[submodules]\t$*" 1>&2
}

# Run git in a submodule. Hooks inherit GIT_DIR, GIT_INDEX_FILE etc. (e.g. during a rebase), which would otherwise
# apply to the submodule.
subgit() {
  local -r path="$1"
  shift
  ( unset $(git rev-parse --local-env-vars); git -C "$path" "$@" )
}

# The commit recorded for submodule $2 by superproject commit $1, or nothing
gitlink() {
  git rev-parse -q --verify "$1:$2" 2> /dev/null || true
}

# The paths of all submodules of the superproject, one per line
submodule_paths() {
  git ls-files --error-unmatch .gitmodules &> /dev/null || return 0
  git config --file .gitmodules --get-regexp '^submodule\..*\.path$' | awk '{ print $2 }'
}

is_null_sha() {
  [[ -z "$1" || "$1" =~ ^0+$ ]]
}

# Whether a remote URL refers to another machine, rather than a repository on the local filesystem.
# Accepts any scheme other than file:// (ssh, git, http(s), remote helpers), and the scp-like [user@]host:path syntax,
# which git only recognises if there is no slash before the first colon.
is_network_url() {
  local url="$1"
  if [[ "$url" == *::* ]]; then
    url="${url#*::}" # <transport>::<address>
  fi
  case "$url" in
    file://*) return 1 ;;
    *://*)    return 0 ;;
  esac
  local -r host="${url%%:*}"
  # exclude local paths, and Windows drive letters (C:\...)
  [[ "$url" == *:* && "$host" != */* && ${#host} -gt 1 ]]
}

# The remotes of submodule $1 whose URL is not on the local filesystem, one per line
network_remotes() {
  local -r path="$1"
  local remote
  for remote in $(subgit "$path" remote); do
    is_network_url "$(subgit "$path" remote get-url "$remote")" && echo "$remote"
  done
  return 0
}

# Initialise any submodule that has not been checked out yet
init_missing_submodules() {
  local path
  # read via fd 3, so that commands in the loop (e.g. an ssh fetch) cannot consume the list
  while read -r path <&3; do
    [[ -z "$path" || -e "$path/.git" ]] && continue
    git submodule update --init ${BUILD_OFFLINE:+--no-fetch} -- "$path"
  done 3< <(submodule_paths)
}

# usage: sync_submodules <new> [<old>...]
#
# Detach each submodule at the commit recorded by superproject commit <new>, but only if it is currently at the commit
# recorded by one of the <old> commits (i.e. it was in step with the superproject before the operation) and has no
# local changes. Otherwise it is left alone with a warning, so that intentional divergence, such as submodule commits
# not yet recorded by the superproject, is never clobbered.
sync_submodules() {
  local -r new="$1"
  shift
  local path want cur old in_step
  while read -r path <&3; do
    [[ -z "$path" ]] && continue
    want="$(gitlink "$new" "$path")"
    [[ -z "$want" ]] && continue
    if [[ ! -e "$path/.git" ]]; then
      git submodule update --init ${BUILD_OFFLINE:+--no-fetch} -- "$path"
      continue
    fi

    cur="$(subgit "$path" rev-parse HEAD)"
    [[ "$cur" == "$want" ]] && continue

    in_step=false
    for old in "$@"; do
      is_null_sha "$old" && continue
      if [[ "$(gitlink "$old" "$path")" == "$cur" ]]; then
        in_step=true
        break
      fi
    done
    if ! $in_step; then
      _log "$path is at ${cur:0:10}, but ${want:0:10} is recorded; leaving it as it was not in step before"
      _log "\tTo sync it, run: git submodule update -- $path"
      continue
    fi
    if [[ -n "$(subgit "$path" status --porcelain --untracked-files=no)" ]]; then
      _log "$path has local changes, so was not moved from ${cur:0:10} to ${want:0:10}"
      continue
    fi

    if ! subgit "$path" cat-file -e "$want^{commit}" 2> /dev/null; then
      if [[ -n "${BUILD_OFFLINE:-}" ]]; then
        _log "$path: ${want:0:10} is not available locally, and BUILD_OFFLINE is defined so not fetching"
        continue
      fi
      subgit "$path" fetch --quiet || true
      if ! subgit "$path" cat-file -e "$want^{commit}" 2> /dev/null; then
        _log "$path: ${want:0:10} could not be found, even after fetching"
        continue
      fi
    fi

    subgit "$path" checkout --quiet --detach "$want"
    _log "$path: ${cur:0:10} -> ${want:0:10}"
  done 3< <(submodule_paths)
}
