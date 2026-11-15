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
## Keep submodules in step with the superproject when HEAD changes (checkout, switch, and the start of a rebase);
## see sync_submodules. post-commit, post-merge and post-rewrite do the same for commits that move HEAD.
##

# Redirect output to stderr.
exec 1>&2

#set -o xtrace
set -o errexit
set -o pipefail
set -o nounset

cd "$(git rev-parse --show-toplevel)"
source "$(dirname "$0")/../cassandra-lib/submodules.sh"

# usage: <previous HEAD> <new HEAD> <1 if HEAD changed, 0 for a file checkout>
if [[ $# -lt 2 ]]; then
  init_missing_submodules # e.g. when first installed by the build
elif [[ "${3:-1}" == 1 ]]; then
  sync_submodules "$2" "$1"
fi
