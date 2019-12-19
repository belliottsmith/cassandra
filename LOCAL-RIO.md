# Run the PR build

To run the PR build locally, run the following command

```
docker run -ti \
  -v $HOME/.m2:/root/.m2:ro \
  -v $PWD:/code \
  -w /code \
  docker.apple.com/cpbuild/applejdk-11:latest \
  rio/release.sh
```