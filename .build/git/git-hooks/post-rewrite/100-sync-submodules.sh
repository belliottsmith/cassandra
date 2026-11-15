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
## After an amend or rebase, move submodules that were in step with the superproject at some point during the
## rewrite to the commit HEAD now records (see sync_submodules). Rebase steps that fast-forward run no hook and are
## not reported to this one, so for a rebase we also consider any commit between the old and new HEAD that changes
## a submodule.
##

# Redirect output to stderr.
exec 1>&2

#set -o xtrace
set -o errexit
set -o pipefail
set -o nounset

cd "$(git rev-parse --show-toplevel)"
source "$(dirname "$0")/../cassandra-lib/submodules.sh"

# usage: amend|rebase, with "<old> <new> [<extra>]" for each rewritten commit on stdin
olds=()
while read -r old new _; do
  olds+=("$old" "$new")
done
orig="$(git rev-parse -q --verify ORIG_HEAD || true)"
if [[ -n "$orig" ]]; then
  olds+=("$orig")
  if [[ "${1:-}" == rebase ]]; then
    base="$(git merge-base HEAD "$orig" || true)"
    [[ -n "$base" ]] && olds+=("$base")
    while read -r commit; do
      olds+=("$commit")
    done < <(git rev-list --max-count=1000 "HEAD...$orig" -- $(submodule_paths))
  fi
fi
sync_submodules HEAD ${olds[@]+"${olds[@]}"}
