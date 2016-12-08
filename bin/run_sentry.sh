#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Execute Sentry command using Maven
# This command can be executed from anywhere as long is cwd is somewhere
# within the git repo

# Allow override of MAIN
SENTRY_MAIN=${SENTRY_MAIN:-org.apache.sentry.SentryMain}
myhome=$(git rev-parse --show-toplevel)

# Locate correct version
# We find the directory in sentry-dist, containing directory "lib"
LIB_DIR=$(dirname $(find ${myhome}/sentry-dist -name lib))

export SENTRY_HOME=${SENTRY_HOME:-${LIB_DIR}}

if [ ! -d ${SENTRY_HOME}/lib ]; then
    echo "can't find sentry lib in $SENTRY_HOME"
    exit 4
fi

# Run SentryMain class. Maven takes care of all classpath dependencies.
mvn -f ${myhome}/sentry-dist/pom.xml exec:java -Dexec.mainClass=${SENTRY_MAIN} \
  -Dexec.args="$*"
