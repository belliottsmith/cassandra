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
## Scenario tests for the git hooks installed by .build/git/install-git-defaults.sh, run in scratch repositories:
## a superproject with a submodule whose upstream is served over smart HTTP (a network remote), and which also has a
## remote on the local filesystem. Requires git (with git-http-backend) and python3.
##
## usage: test-git-hooks.sh [--without-hooks] [--keep]
##   --without-hooks   run without installing the hooks, to confirm the tests detect their absence (expect failures)
##   --keep            keep the scratch directory
##

set -o nounset
set -o pipefail

bin="$(cd "$(dirname "$0")" > /dev/null; pwd)"
readonly installer="$bin/../install-git-defaults.sh"

with_hooks=true
keep=false
for arg in "$@"; do
  case "$arg" in
    --without-hooks) with_hooks=false ;;
    --keep)          keep=true ;;
    -h|--help)       sed -n '/^## usage/,/^##$/p' "$0" | sed 's/^## \{0,1\}//'; exit 0 ;;
    *)               echo "Unknown argument: $arg" 1>&2; exit 2 ;;
  esac
done

root="$(mktemp -d "${TMPDIR:-/tmp}/test-git-hooks.XXXXXX")"
server_pid=""
cleanup() {
  [[ -n "$server_pid" ]] && kill "$server_pid" 2> /dev/null
  if $keep; then echo "Kept $root"; else rm -rf "$root"; fi
}
trap cleanup EXIT

# isolate from the user's git configuration (e.g. signing, hooksPath, default branch)
export GIT_CONFIG_NOSYSTEM=1
export GIT_CONFIG_GLOBAL="$root/gitconfig"
git config --global user.name "Test"
git config --global user.email "test@example.com"
git config --global init.defaultBranch trunk
git config --global advice.detachedHead false
unset $(git rev-parse --local-env-vars)

pass=0
fail=0
check() { # check <description> <command...>
  local -r description="$1"
  shift
  if "$@"; then
    echo "   PASS: $description"
    pass=$((pass + 1))
  else
    echo "   FAIL: $description"
    fail=$((fail + 1))
  fi
}

# the scratch submodule commit with subject $1
S() { git -C "$root/work-sub" rev-list -1 --all --grep="^$1\$"; }
link() { git rev-parse HEAD:sub; }
cur() { git -C sub rev-parse HEAD; }
in_sync() { [[ "$(link)" == "$(cur)" ]]; }
detached() { ! git -C sub symbolic-ref -q HEAD > /dev/null; }
clean() { [[ -z "$(git status --porcelain --untracked-files=no)" ]]; }
quietly() { "$@" > /dev/null 2>&1; }

# submodule commits s1, s2, s3
git init -q "$root/work-sub"
for s in s1 s2 s3; do
  git -C "$root/work-sub" commit -q --allow-empty -m "$s"
done

mkdir -p "$root/srv"
python3 "$bin/git-http-server.py" "$root/srv" "$root/port" &
server_pid=$!
for _ in $(seq 100); do
  [[ -f "$root/port" ]] && break
  sleep 0.1
done
[[ -f "$root/port" ]] || { echo "The git http server did not start" 1>&2; exit 1; }
readonly upstream="http://127.0.0.1:$(cat "$root/port")/sub.git"

# A superproject with branches:
#   feature: p0 (sub=s1) - f1 - f2 (bumps sub to s2) - f3
#   base:    p0 (sub=s1) - b1
# The submodule's upstream (origin) is served over HTTP; it also has a remote on the local filesystem (local).
fixture() {
  cd "$root" && rm -rf super super-remote.git wt srv/sub.git sub-mirror.git
  git clone -q --bare work-sub srv/sub.git && git -C srv/sub.git config http.receivepack true
  git clone -q --bare work-sub sub-mirror.git
  git init -q --bare super-remote.git
  git init -q super && cd super
  quietly git submodule add -q "$upstream" sub
  git -C sub remote add local "$root/sub-mirror.git"
  git -C sub fetch -q local
  git config submodule.recurse true
  git remote add origin "$root/super-remote.git"
  if $with_hooks; then
    quietly "$installer" || { echo "The installer failed" 1>&2; exit 1; }
  fi
  git -C sub checkout -q "$(S s1)"; git add sub; git commit -q -m "p0 sub=s1"; P0="$(git rev-parse HEAD)"
  git switch -q -c feature; echo a > a; git add a; git commit -q -m "f1 file"
  git -C sub checkout -q "$(S s2)"; git add sub; git commit -q -m "f2 bump sub=s2"
  echo b > b; git add b; git commit -q -m "f3 file"
  quietly git switch -q -c base "$P0"; echo z > z; git add z; git commit -q -m "b1 file"
  quietly git switch -q feature
}

echo "1) rebase replaying a pointer bump onto a base without it"
fixture
quietly git rebase -q base
check "in sync after rebase" in_sync
check "superproject clean" clean
check "detached" detached

echo "2) rebase -i onto base, 'edit' the bump"
fixture
GIT_SEQUENCE_EDITOR="sed -i.bak '/f2 bump/s/^pick/edit/'" quietly git rebase -q -i base
check "stopped at the bump" test "$(git log -1 --format=%s)" = "f2 bump sub=s2"
check "in sync at the stop" in_sync
quietly git rebase --continue
check "in sync after --continue" in_sync

echo "3) rebase -i (same base) dropping the bump"
fixture
GIT_SEQUENCE_EDITOR="sed -i.bak '/f2 bump/d'" quietly git rebase -q -i "$P0"
check "in sync" in_sync
check "superproject clean" clean

echo "4) rebase -i (same base, so fast-forwarded picks), 'edit' f1, commit -a --amend, continue"
fixture
GIT_SEQUENCE_EDITOR="sed -i.bak '/f1 file/s/^pick/edit/'" quietly git rebase -q -i "$P0"
check "in sync at the stop" in_sync
echo x >> a; git commit -q -a --amend --no-edit
check "amended f1 still records s1" test "$(git rev-parse HEAD:sub)" = "$(git rev-parse "$P0:sub")"
quietly git rebase --continue
check "in sync after --continue" in_sync

echo "5) developing in the submodule: a new commit on a branch, then an unrelated superproject commit"
fixture
git -C sub switch -q -c work; git -C sub commit -q --allow-empty -m s4; S4="$(cur)"
echo c > c; git add c; out="$(git commit -q -m unrelated 2>&1)"
check "submodule left at s4" test "$(cur)" = "$S4"
check "submodule still on its branch" test "$(git -C sub branch --show-current)" = work
check "warned" grep -q "leaving it" <<< "$out"
git add sub; git commit -q --amend -m "record s4"
check "in sync after recording s4" in_sync

echo "6) switching branches"
fixture
quietly git switch -q base
check "in sync on base" in_sync
quietly git switch -q feature
check "in sync on feature" in_sync
check "detached" detached

echo "7) cherry-pick the bump onto base"
fixture
quietly git switch -q base
quietly git cherry-pick "$(git rev-parse feature~1)"
check "in sync" in_sync

echo "8) merge feature into base"
fixture
quietly git switch -q base
quietly git merge -q --no-edit feature
check "in sync" in_sync
check "records s2" test "$(link)" = "$(git rev-parse feature:sub)"

echo "9) a file checkout does not move the submodule"
fixture
git -C sub checkout -q "$(S s3)"; echo y > a; quietly git checkout -- a
check "submodule left at s3" test "$(cur)" = "$(S s3)"

echo "10) the rewind guard"
fixture
git -C sub checkout -q "$(S s1)"; echo y >> b
out="$(git commit -q -am "would rewind" 2>&1)"; rc=$?
check "commit -a staging an older submodule commit is refused" test $rc -ne 0
check "explains how to recover" grep -q "git restore --staged sub" <<< "$out"
CASSANDRA_ALLOW_SUBMODULE_REWIND=1 quietly git commit -q -am "intended rewind"
check "allowed with CASSANDRA_ALLOW_SUBMODULE_REWIND=1" test "$(link)" = "$(S s1)"
git -C sub checkout -q "$(S s3)"; git add sub; git commit -q -m "forward"
check "moving forward is allowed" test "$(link)" = "$(S s3)"

echo "11) linked worktrees"
fixture
quietly git worktree add -q "$root/wt" base
pushd "$root/wt" > /dev/null
check "worktree submodule initialised" test -e sub/.git
check "worktree in sync" in_sync
git -C sub checkout -q "$(S s2)"; git add sub; git commit -q -m "bump in worktree"
quietly git switch -q --detach "$P0"
check "hooks run in the worktree (synced on switch)" in_sync
popd > /dev/null

echo "12) pre-push"
fixture
rm -f "$root/user-hook-stdin"; mkdir -p .git/hooks/pre-push.d
printf '#!/bin/sh\ncat > "%s"\n' "$root/user-hook-stdin" > .git/hooks/pre-push.d/000-original.sh
chmod +x .git/hooks/pre-push.d/000-original.sh
git -C sub switch -q -c feature; git -C sub commit -q --allow-empty -m s4; S4="$(cur)"
git -C sub push -q local feature; git add sub; git commit -q -m "bump sub=s4"
out="$(git push -q origin feature 2>&1)"; rc=$?
check "refused when only on a filesystem remote" test $rc -ne 0
check "names the unpublished commit" grep -q "sub $S4" <<< "$out"
check "a relocated user hook runs first, and receives git's stdin" grep -q "refs/heads/feature" "$root/user-hook-stdin"
check "CASSANDRA_SKIP_SUBMODULE_PUSH_CHECK skips the check" quietly env CASSANDRA_SKIP_SUBMODULE_PUSH_CHECK=1 git push -q origin feature:skipped-check
git -C sub push -q origin feature 2> /dev/null
check "allowed once on the network remote" quietly git push -q origin feature
git -C sub commit -q --allow-empty -m s5; git add sub; git commit -q -m "bump sub=s5"
git config cassandra.submodules.push on-demand
check "on-demand: pushes the submodule branch and succeeds" quietly git push -q origin feature
check "on-demand: s5 now upstream" git -C "$root/srv/sub.git" cat-file -e "$(cur)^{commit}"
git -C sub commit -q --amend --allow-empty -m "s5 rewritten"; git add sub; git commit -q --amend -m "bump sub=s5'"
check "on-demand: a forced superproject push force-pushes the rewritten submodule branch" quietly git push -q --force origin feature
check "on-demand: upstream feature is the rewritten commit" test "$(git -C "$root/srv/sub.git" rev-parse feature)" = "$(cur)"
git config --unset cassandra.submodules.push
git -C sub commit -q --allow-empty -m s6; git add sub; git commit -q -m "bump sub=s6"
check "--no-verify skips the check" quietly git push -q --no-verify origin feature

echo
echo "passed=$pass failed=$fail"
[[ $fail -eq 0 ]]
