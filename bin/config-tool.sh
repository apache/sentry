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

_CMD_JAR=sentry-binding-hive-*.jar
_HIVE_CMD=${HIVE_HOME}/bin/hive
for f in ${SENTRY_HOME}/lib/*.jar; do
  HADOOP_CLASSPATH=${HADOOP_CLASSPATH}:${f}
done

${_HIVE_CMD} --service jar ${SENTRY_HOME}/lib/${_CMD_JAR} org.apache.sentry.binding.hive.authz.SentryConfigTool "$@"

