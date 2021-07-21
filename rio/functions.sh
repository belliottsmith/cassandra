_abspath() {
  echo "$(cd "$(dirname "$1")"; pwd)/$(basename "$1")"
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
  rio/build-dtest-jar.sh || ant dtest-jar
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
