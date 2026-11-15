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
## A commit may record a different submodule commit than the one checked out, e.g. when cherry-picked or replayed
## by a rebase; move submodules that were in step with the parent to the commit now recorded (see sync_submodules).
##

# Redirect output to stderr.
exec 1>&2

#set -o xtrace
set -o errexit
set -o pipefail
set -o nounset

cd "$(git rev-parse --show-toplevel)"
source "$(dirname "$0")/../cassandra-lib/submodules.sh"

sync_submodules HEAD "$(git rev-parse -q --verify HEAD~1 || true)"
