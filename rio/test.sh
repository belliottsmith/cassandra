set -e

cleanup() {
    if [ -d build/test/output ]; then
        mkdir -p .out/test-results
        cp build/test/output/*.xml .out/test-results/
    fi
}

trap cleanup 0

export JAVA_TOOL_OPTIONS=-Dfile.encoding=UTF8
ant realclean
ant test
ant build
