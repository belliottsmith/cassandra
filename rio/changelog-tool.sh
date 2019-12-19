#!/usr/bin/env bash

# Generates changelogs for new releases.

# set -o xtrace
set -o nounset
set -o errexit

# HELPERS

generate_changelog(){
    start_commit="$1"
    end_commit="$2"

    ignore_commit_subject_regex='^Bumping version from [0-9.]+ to [0-9.]+ after release$'

    # Need to make sure the Rio pipeline has checkout:fetchTags=true so start_commit is available; otherwise will fail
    commits=$(git log --format=%h "${start_commit}".."${end_commit}")
    for commit in $commits; do
        subject=$(git log -1 --format=%s "${commit}")

        # Skip certain commits in changelog
        if ! [[ "${subject}" =~ ${ignore_commit_subject_regex} ]]; then
            echo "* ${subject}"
        fi
    done
}

# COMMANDS

command_expand_macros(){
    changelog_file="$1"

    # Search for AUTOGEN_CHANGELOG_COMMITS macro
    autogen_macro_regex='^AUTOGEN_CHANGELOG_COMMITS ([^ ]+) ([^ ]+)$'
    local autogen_macro_matches
    autogen_macro_matches="$(grep --extended-regexp "${autogen_macro_regex}" "${changelog_file}" || echo -n '')"

    if [ -z "${autogen_macro_matches}" ]; then
        # No macros found, just output the plain file
        cat "${changelog_file}"
        exit 0
    fi

    if [ "$(echo "${autogen_macro_matches}" | wc -l)" -gt 1 ]; then
        echo 'Found multiple macros in the input text. Only a single macro can be expanded at a time.' >&2
        exit 1
    fi

    # Get commits referenced in macro
    if [[ "${autogen_macro_matches}" =~ ${autogen_macro_regex} ]]; then
        start_commit=${BASH_REMATCH[1]}
        end_commit=${BASH_REMATCH[2]}
    else
        echo 'Something went wrong. Grep and Bash do not agree whether the changelog has macros matching a regex. Requires debugging.' >&2
        exit 1
    fi

    # Output the generated changelog with macros replaced
    while IFS= read -r line; do
        if [[ "${line}" =~ ${autogen_macro_regex} ]]; then
            generate_changelog "${start_commit}" "${end_commit}"
        else
            echo "${line}"
        fi
    done < "${changelog_file}"
}

command_help(){
    cat <<HEREDOC
Generates changelogs for new releases.

Usage:

expand_macros [changelog-file]

  Expands macros in a changelog file and output the full expanded result to stdout. Currently, there is only a single macro: AUTOGEN_CHANGELOG_COMMITS <start_commit> <end_commit>.
  This macro expands to a bullet-form list of commits between <start_commit> and <end_commit>, and validates commit messages before proceeding.

  Currently, only a single macro is supported.

HEREDOC
}

# ENTRYPOINT

command="$1"
shift
case ${command} in
    "" | "help" | "-h" | "--help")
        command_help
        ;;
    *)
        "command_${command}" "$@"

        if [ "$?" = 127 ]; then
            command_help
            exit 1
        fi
        ;;
esac
