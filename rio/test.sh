set -e

cleanup() {
    if [ -d build/test/output ]; then
        mkdir -p .out/test-results
        cp build/test/output/*.xml .out/test-results/
    fi
}

trap cleanup 0

export JAVA_TOOL_OPTIONS=-Dfile.encoding=UTF8


# We check here that the rio-build.xml isn't missing some change from build.xml, not full proof but should catch 99% of misssed dep changes.
diff --ignore-space-change --ignore-matching-lines='groupId="com.apple.cie.db.cassandra"\|groupId="org.apache.cassandra"\|default="jar" name="cie-cassandra"\|default="jar" name="apache-cassandra"\|Dcie-cassandra.sizeTieredDefault=\|cie-cassandra.disable_schema_drop_log=' rio-build.xml build.xml
ret=$?
if [ ${ret} -ne 0 ]; then
    echo "found unexpected diff between rio-build.xml build.xml, diff ignoring expected differences:"
    diff --ignore-space-change --ignore-matching-lines='groupId="com.apple.cie.db.cassandra"\|groupId="org.apache.cassandra"\|default="jar" name="cie-cassandra"\|default="jar" name="apache-cassandra"\|Dcie-cassandra.sizeTieredDefault=\|cie-cassandra.disable_schema_drop_log=' rio-build.xml build.xml
    echo "full diff between rio-build.xml build.xml :"
    diff rio-build.xml build.xml
    echo "Now exiting and not building/testing"
    exit 1
fi


ant realclean
ant test
ant build
