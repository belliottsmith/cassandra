#! /bin/bash

#
# Bump the patch-level version in CIE-VERSION and prepare CIE-CHANGES.txt for the next release.
# Called by rio/release.sh
#

set -o errexit
set -o nounset
set -o pipefail

CURRENTVERSION="${1:-$(<CIE-VERSION)}"

IFS="-" read -r dotted suffix < CIE-VERSION <<< "$CURRENTVERSION"
IFS="." read -r major minor micro patch rest < CIE-VERSION <<< "$dotted"

nextpatch=$((patch + 1))

case "${suffix}" in
  "" | hotfix)
    ;;

  *)
    echo "Unable to bump release version for ${CURRENTVERSION} - only release branch and hotfix branch supported.  Update $0 to fix." >&2
    exit 1
    ;;
esac


# Use the shell test mechanism to parse each of the version components, which
# will fail if they are not integers and check there are no additional components
#
if [ "$major" -ne "$major" ] || [ "$minor" -ne "$minor" ] || \
   [ "$micro" -ne "$micro" ] || [ "$patch" -ne "$patch" ] || [ "$rest" != "" ]
then
    echo "Unable to bump release version for ${CURRENTVERSION} - can only handle simple major, minor, micro, patch scheme" >&2
    exit 1
fi

NEWVERSION="${major}.${minor}.${micro}.${nextpatch}${suffix:+-}${suffix:-}"

echo "$NEWVERSION" > CIE-VERSION

# Prepare the CHANGES.txt (for 3.0) / CIE-CHANGES.txt file for the next release
CHANGE_FILE="CIE-CHANGES.txt"
if [ ! -e "$CHANGE_FILE" ]; then
  CHANGE_FILE="CHANGES.txt"
fi
OLDCHANGES="/tmp/${CHANGE_FILE}.$$"
cp -f "$CHANGE_FILE" "$OLDCHANGES"
cat > "$CHANGE_FILE" <<EOF
${NEWVERSION}

$(cat "${OLDCHANGES}")
EOF

git add CIE-VERSION ${CHANGE_FILE}
git commit -m "Bumping version from $CURRENTVERSION to $NEWVERSION after release

The Rio release step automatically bumps the patch level version after cutting
a full release to make sure future snapshot versions are named after the release
they are targetting"

git push
