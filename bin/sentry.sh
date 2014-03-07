#!/bin/bash
#
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

################################
# functions
################################

info() {
  local msg=$1
  echo "Info: $msg" >&2
}

warn() {
  local msg=$1
  echo "Warning: $msg" >&2
}

error() {
  local msg=$1
  local exit_code=$2

  echo "Error: $msg" >&2

  if [ -n "$exit_code" ] ; then
    exit $exit_code
  fi
}

################################
# main
################################

# set default params
SENTRY_CLASSPATH=""

if [ -z "${SENTRY_HOME}" ]; then
  SENTRY_HOME=$(cd $(dirname $0)/..; pwd)
fi

if [ -f ${SENTRY_HOME}/etc/sentry-env.sh ]
then
  . ${SENTRY_HOME}/etc/sentry-env.sh
fi

# prepend $SENTRY_HOME/lib jars to the specified classpath (if any)
if [ -n "${SENTRY_CLASSPATH}" ] ; then
  SENTRY_CLASSPATH="${SENTRY_HOME}/lib/*:$SENTRY_CLASSPATH"
else
  SENTRY_CLASSPATH="${SENTRY_HOME}/lib/*"
fi

# find java
if [ -z "${JAVA_HOME}" ] ; then
  warn "JAVA_HOME is not set!"
  # Try to use Bigtop to autodetect JAVA_HOME if it's available
  if [ -e /usr/libexec/bigtop-detect-javahome ] ; then
    . /usr/libexec/bigtop-detect-javahome
  elif [ -e /usr/lib/bigtop-utils/bigtop-detect-javahome ] ; then
    . /usr/lib/bigtop-utils/bigtop-detect-javahome
  fi

  # Using java from path if bigtop is not installed or couldn't find it
  if [ -z "${JAVA_HOME}" ] ; then
    JAVA_DEFAULT=$(type -p java)
    [ -n "$JAVA_DEFAULT" ] || error "Unable to find java executable. Is it in your PATH?" 1
    JAVA_HOME=$(cd $(dirname $JAVA_DEFAULT)/..; pwd)
  fi
fi

# look for hadoop libs
HADOOP_IN_PATH=$(PATH="${HADOOP_HOME:-${HADOOP_PREFIX}}/bin:$PATH" \
    which hadoop 2>/dev/null)
if [ ! -f "${HADOOP_IN_PATH}" ]; then
  error "Cannot find Hadoop command in path"
fi

info "Including Hadoop libraries found via ($HADOOP_IN_PATH)"

# determine hadoop classpath
HADOOP_CLASSPATH=$($HADOOP_IN_PATH classpath)

# hack up and filter hadoop classpath
ELEMENTS=$(sed -e 's/:/ /g' <<<${HADOOP_CLASSPATH})
for ELEMENT in $ELEMENTS; do
  for PIECE in $(echo $ELEMENT); do
    if [[ $PIECE =~ slf4j-(api|log4j12).*\.jar ]]; then
      info "Excluding $PIECE from classpath"
      continue
    else
      SENTRY_CLASSPATH="$SENTRY_CLASSPATH:$PIECE"
    fi
  done
done
exec $JAVA_HOME/bin/java $SENTRY_OPTS -cp "$SENTRY_CLASSPATH" org.apache.sentry.SentryMain "$@"
