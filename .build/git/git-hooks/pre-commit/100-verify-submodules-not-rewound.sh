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
## Refuse a commit that moves a submodule back to an ancestor of the commit HEAD records. This is almost never intended,
## and is what happens when a stale submodule checkout (e.g. left behind by a rebase) is staged by "git commit -a".
##
## To commit an intended rewind, set CASSANDRA_ALLOW_SUBMODULE_REWIND=1 (or use --no-verify); to disable this check, run
##   git config --local cassandra.submodules.verify-not-rewound false
##

# Redirect output to stderr.
exec 1>&2

#set -o xtrace
set -o errexit
set -o pipefail
set -o nounset

cd "$(git rev-parse --show-toplevel)"
source "$(dirname "$0")/../cassandra-lib/submodules.sh"

[[ -n "${CASSANDRA_ALLOW_SUBMODULE_REWIND:-}" ]] && exit 0
[[ "$(git config --bool cassandra.submodules.verify-not-rewound || echo true)" == false ]] && exit 0
git rev-parse -q --verify HEAD > /dev/null || exit 0 # initial commit

status=0
while read -r path <&3; do
  [[ -z "$path" || ! -e "$path/.git" ]] && continue
  staged="$(git rev-parse -q --verify ":$path" 2> /dev/null || true)"
  recorded="$(gitlink HEAD "$path")"
  [[ -z "$staged" || -z "$recorded" || "$staged" == "$recorded" ]] && continue
  if subgit "$path" merge-base --is-ancestor "$staged" "$recorded" 2> /dev/null; then
    _log "This commit would move $path back from ${recorded:0:10} to ${staged:0:10}, an ancestor of it."
    _log "\tThis usually means a stale submodule checkout (e.g. after a rebase) was staged; to restore it, run"
    _log "\t\tgit restore --staged $path && git submodule update -- $path"
    _log "\tIf the rewind is intended, commit with CASSANDRA_ALLOW_SUBMODULE_REWIND=1 (or --no-verify)"
    status=1
  fi
done 3< <(submodule_paths)
exit $status
