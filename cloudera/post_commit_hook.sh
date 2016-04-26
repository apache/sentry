#!/bin/bash
set -x
echo "Running tests using JDK7"

export JAVA7_BUILD=true
source /mnt/toolchain/toolchain.sh

find . -name test-classes | grep target/test-classes | xargs rm -rf

mvn clean compile package -DskipTests=true

# enable full tests for now
mvn test -PskipOnlySlowTests --fail-fast -DtestFailureIgnore=false

# validate test status and email failures to author
res=$?
if [[ "${res}" -eq 0 ]]; then
  exit 0
fi
if [[ -z "${BUILD_URL}" ]] || [[ "${BUILD_URL}xxx" = "xxx" ]] ; then
  BUILD_URL="http://unittest.jenkins.cloudera.com/job/CDH5-Sentry-Post-Commit/lastBuild"
fi
echo "Please check out details here: ${BUILD_URL}" > /tmp/sentry-unit-tests.log
EML="sentry-jenkins@cloudera.com"
if [[ ! -z "${GIT_BRANCH}" ]] && [[ "${GIT_BRANCH}xxx" != "xxx" ]] ; then
  EML=$(git shortlog ${GIT_BRANCH} -1 -se | awk -F"<" '{print $2}' | awk -F">" '{print $1}')
fi
sudo mkdir -p /etc/ssmtp
sudo echo "root=postmaster" > /etc/ssmtp/ssmtp.conf
sudo echo "mailhub=cloudera.com:25" >> /etc/ssmtp/ssmtp.conf
mail -s "Most Recent Sentry Post Commit Job Failed" ${EML} < /tmp/sentry-unit-tests.log
exit ${res}
