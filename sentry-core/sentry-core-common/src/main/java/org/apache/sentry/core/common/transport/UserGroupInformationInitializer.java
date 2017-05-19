/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.sentry.core.common.transport;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION;

/**
 * Wrapper to initialize UserGroupInformation
 */

public class UserGroupInformationInitializer {

  // initialize() method could be called my multiple threads.
  // to attain visibility guarantee on isInitialized, it is declared volatile.
  private static volatile boolean isInitialized = false;

  // initialization block may be executed multiple times. This is fine as setConfiguration is
  // thread-safe
  public static void initialize(Configuration conf) {
    if(!isInitialized) {
      Configuration newConf = new Configuration(conf);
      // When kerberos is enabled,  UserGroupInformation should have been initialized with
      // HADOOP_SECURITY_AUTHENTICATION property. There are instances where this is not done.
      // Example: Solr and Kafka while using sentry generic clients were not updating this
      // property. Instead of depending on the callers to update this configuration and to be
      // sure that UserGroupInformation is properly initialized, sentry client is explicitly
      // doing it,
      newConf.set(HADOOP_SECURITY_AUTHENTICATION, SentryClientTransportConstants.KERBEROS_MODE);
      UserGroupInformation.setConfiguration(newConf);
      isInitialized = true;
    }
  }
}