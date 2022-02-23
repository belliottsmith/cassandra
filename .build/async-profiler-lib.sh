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

#set -o xtrace
set -o errexit
set -o pipefail
set -o nounset

bin="$(cd "$(dirname "$0")/.." > /dev/null; pwd)"

readonly BASE_URL="https://artifacts.apple.com/github-releases"

download_mac() {
  local -r version="$1"
  local -r out_dir="$2"

  IFS="." read -r major minor patch <<< "$version"
  # doesn't support 2.5 as thats actually tar... but... KISS?
  if [ $major -ge 2 ] && [ $minor -ge 5 ]; then
    local -r url="${BASE_URL}/jvm-profiling-tools/async-profiler/releases/download/v${version}/async-profiler-${version}-macos.zip"
    local -r path="async-profiler-${version}-macos/build"

    curl -s -O -L "$url"
    unzip "async-profiler-${version}-macos.zip"
    # rename .so to .dylib as dylib is what the JVM searches for on mac
    mv "$path"/libasyncProfiler.so "${out_dir}"/libasyncProfiler.dylib

    rm -rf "$(dirname "$path")" "async-profiler-${version}-macos.zip"
  else
    local -r url="${BASE_URL}/jvm-profiling-tools/async-profiler/releases/download/v${version}/async-profiler-${version}-macos-x64.tar.gz"
    local -r path="async-profiler-${version}-macos-x64/build"

    curl -s -O -L "$url"
    tar zxf "async-profiler-${version}-macos-x64.tar.gz" "$path/libasyncProfiler.so"
    # rename .so to .dylib as dylib is what the JVM searches for on mac
    mv "$path"/libasyncProfiler.so "${out_dir}"/libasyncProfiler.dylib

    rm -rf "$(dirname "$path")" "async-profiler-${version}-macos-x64.tar.gz"
  fi
}

download_linux() {
  local -r version="$1"
  local -r out_dir="$2"

  local -r url="${BASE_URL}/jvm-profiling-tools/async-profiler/releases/download/v${version}/async-profiler-${version}-linux-x64.tar.gz"
  local -r path="async-profiler-${version}-linux-x64/build"

  curl -s -O -L "$url"
  tar zxf "async-profiler-${version}-linux-x64.tar.gz"
  mv "$path"/libasyncProfiler.so "${out_dir}"/
  mv "$path"/async-profiler.jar "$bin/lib/async-profiler-${version}.jar"
  ln "$bin/lib/async-profiler-${version}.jar" "$bin/build/lib/jars/async-profiler-${version}.jar"
  
  rm -rf "$(dirname "$path")" "async-profiler-${version}-linux-x64.tar.gz"
}

_main() {
  local -r version="${1:-1.8.3}"
  local -r out_dir="$bin/src/resources/META-INF/native"
  mkdir -p "$out_dir" || true
  rm "$out_dir"/libasyncProfiler.* "$bin/lib/"async-profiler*.jar || true
  download_mac "$version" "$out_dir"
  download_linux "$version" "$out_dir"
}

_main "$@"
