# Local Rio

Our rio builds are actually build scripts run inside a docker container; for this reason its simple to replicate the Rio environment by running the builds inside normal containers; just make sure docker is allocated enough resources.

## Test

```
git worktree remove --force rio-build || true
git worktree add rio-build
cd rio-build
docker run -t -i \
  --rm \
  -w $PWD \
  -v $PWD:$PWD \
  -v $HOME/.m2:/root/.m2:ro \
  docker.apple.com/pie/cie-cassandra-build-jdk-8:latest \
  bash -c "set -x;
sed -i 's;${basedir}/build;/tmp/build;g' build.xml
sed -i 's;${basedir}/build;/tmp/build;g' rio-build.xml

cleanup() {
  if [ -d /tmp/build/test/output/ ]; then
    mkdir -p build/test/output
    cp /tmp/build/test/output/*.xml build/test/output/
  fi
}
trap cleanup 0

rio/test.sh
"
```
