#/bin/bash -e

sonar_scanner() {
  local params=$@

  sonar-scanner \
    -Dsonar.host.url='https://sonarqube.split-internal.com' \
    -Dsonar.login="$SONAR_TOKEN" \
    -Dsonar.ws.timeout='300' \
    -Dsonar.sources='splitio' \
    -Dsonar.projectName='python-client' \
    -Dsonar.projectKey='python-client' \
    _Dsonar.python.coverage.reportPaths='coverage.xml' \
    -Dsonar.links.ci='https://travis-ci.com/splitio/python-client' \
    -Dsonar.links.scm='https://github.com/splitio/python-client' \
    ${params}

  return $?
}

if [ "$TRAVIS_PULL_REQUEST" != "false" ]; then
  sonar_scanner \
    -Dsonar.pullrequest.provider='GitHub' \
    -Dsonar.pullrequest.github.repository='splitio/python-client' \
    -Dsonar.pullrequest.key=$TRAVIS_PULL_REQUEST \
    -Dsonar.pullrequest.branch=$TRAVIS_PULL_REQUEST_BRANCH \
    -Dsonar.pullrequest.base=$TRAVIS_BRANCH
else
  if [ "$TRAVIS_BRANCH" == 'master' ]; then
    sonar_scanner \
      -Dsonar.branch.name=$TRAVIS_BRANCH
  else
    if [ "$TRAVIS_BRANCH" == 'development' ]; then
      TARGET_BRANCH='master'
    else
      TARGET_BRANCH='development'
    fi
    sonar_scanner \
      -Dsonar.branch.name=$TRAVIS_BRANCH \
      -Dsonar.branch.target=$TARGET_BRANCH
  fi
fi
