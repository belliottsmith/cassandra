#!/usr/bin/env bash

#set -o xtrace
set -o errexit
set -o pipefail
set -o nounset

_main() {
    ant testsome -Dtest.name=org.apache.cassandra.distributed.fuzz.ConcurrentQuiescentCheckerIntegration -Dtest.timeout=3000000 -Dtest.showoutput=true
}

_main "$@"
