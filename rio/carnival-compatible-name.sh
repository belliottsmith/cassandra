#!/usr/bin/env bash
#
# This script tries to reproduce the logic from dna/src/com/apple/cie/db/dna/logic/LogicUtils.java under cie-db,
# warts and all.
#
set -o errexit
set -o nounset
set -o pipefail

readonly -a units=(zero one two three four five six seven eight nine ten eleven twelve thirteen fourteen fifteen sixteen seventeen eighteen nineteen)
readonly -a tens=("" "" twenty thirty forty fifty sixty seventy eighty ninety)

function convertToNumber {
    local -r number="$1"

    if ((number < 0))
    then
        echo "$number is less than zero, cannot convert" >&2
        exit 1
    elif ((number > 99))
    then
        # echo "$number is greater than ninety nine, cannot convert" >&2
        # exit 1
        echo fizbuz
    elif ((number < 20))
    then
    echo "${units[$number]}"
    elif ((number % 10 == 0))
    then
    echo "${tens[$((number/10))]}"
    else
    echo "${tens[$((number/10))]}${units[$((number%10))]}"
    fi

}

function getCarnivalCompatiblePackageName {
    local -r cassandraVersion="$1"

    local numberBuilder=""
    local i=0
    while [ $i -lt ${#cassandraVersion} ]
    do
        local c="${cassandraVersion:$i:1}"
        case "$c" in
            [0-9])
                numberBuilder="${numberBuilder}$c"
                ;;
            *)
                if [ -n "${numberBuilder}" ]
                then
                    echo -n "$(convertToNumber "$numberBuilder")"
                    numberBuilder=""
                fi

                case "$c" in
                    .)
                        echo -n "-"
                        ;;

                    *)
                        echo -n "${c}" | tr '[:upper:]' '[:lower:]'
                        ;;
                esac
        esac
        i=$((i+1))
    done
    if [ -n "${numberBuilder}" ]
    then
        echo -n "$(convertToNumber "$numberBuilder")"
    fi
}


if [ -n "${1:-}" ]
then
    getCarnivalCompatiblePackageName "$1"
else
    echo
    echo "Requires a single argument of the cassandra version to convert, for example."
    echo
    echo "  \$ ${0} 3.0.19.0" >&2
    exit 2
fi
