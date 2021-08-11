_abspath() {
  echo "$(cd "$(dirname "$1")"; pwd)/$(basename "$1")"
}

warn() {
  echo "$*" 1>&2
}

_is_xtrace_enabled() {
  # copied from https://unix.stackexchange.com/questions/21922/bash-test-whether-original-script-was-run-with-x
  case "$-" in
    *x*) echo "true" ;;
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
  local -r repo="$1"
  local -r version="$2"
  local -r output_dir="$3"

  # prefix stdout and stderr with the branch being built so its clear where logs are coming from
  exec > >( awk "{ print \"dtest-$version.jar> \", \$0 } " )
  exec 2> >( awk "{ print \"dtest-$version.jar> \", \$0 } " )

  # why hard code github?  If a Apple fork is used, the fork may be very out of date with the
  # different branches which could lead to failing tests caused by out dated dtest jars to avoid
  # this, rely on github
  clone_cassandra "$repo" "$version" "/tmp/$version"
  cd "/tmp/$version"
  # make sure to build jars outside of parallel ci since network access is more limited
  ant dtest-jar
  cp build/dtest*.jar "$output_dir"
}

_parallel_clone_branches() {
  local -r output_dir="$1"; shift

  mkdir -p "$output_dir"
  pids=()
  local repo
  local branch
  for line in "$@"; do
    repo="$(echo "$line" | awk '{print $1}')"
    branch="$(echo "$line" | awk '{print $2}')"
    _download_and_build "$repo" "$branch" "$output_dir" &
    pids+=( $! )
  done
  for pid in "${pids[@]}"; do
    wait $pid
  done
}

_verify_command_success() {
  local -r output_dir="$1"
  # sample file parallel-output/unit/java=11/map-0-output/.command/3/rc
  local fail=false
  local dir
  local rc_file
  local rc
  local stdout
  local stderr
  local xtrace_enabled=false
  if [[ $(_is_xtrace_enabled) == true ]]; then
    xtrace_enabled=true
    set +x
  fi

  for command_dir in $(find "$output_dir" -name '.command' -type d); do
    for command in $(ls -1 "$command_dir"); do
      dir="${command_dir}/${command}"
      if [[ ! -d "$dir" ]]; then
        # skip files such as files.txt
        continue
      fi
      rc_file="${dir}/rc"
      stdout="${dir}/stdout"
      stderr="${dir}/stderr"
      if [[ ! -e "$rc_file" ]]; then
        fail=true
        warn "Unable to find rc file for command $dir"
        continue
      fi
      rc="$(cat "$rc_file")"
      if [[ $rc -ne 0 ]]; then
        fail=true
        warn "command $dir had rc=$rc"
        if [[ -e "$stderr" ]]; then
          warn "command $dir; stderr"
          cat "$stderr" 1>&2
        else
          warn "command $dir does not have stderr"
        fi
        if [[ -e "$stdout" ]]; then
          warn "command $dir; stdout"
          cat "$stdout" 1>&2
        else
          warn "command $dir does not have stdout"
        fi
      fi
    done
  done
  if [[ $xtrace_enabled  == true ]]; then
    set -x
  fi
  if [[ $fail == true ]]; then
    return 1
  fi
}
