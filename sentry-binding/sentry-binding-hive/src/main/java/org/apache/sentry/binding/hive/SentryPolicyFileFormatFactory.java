/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.sentry.binding.hive;

import java.lang.reflect.Constructor;

import org.apache.sentry.binding.hive.conf.HiveAuthzConf;
import org.apache.sentry.binding.hive.conf.HiveAuthzConf.AuthzConfVars;

/**
 * SentryPolicyFileFormatFactory is used to create FileFormatter for different file type according
 * to the configuration, the default FileFormatter is for ini file.
 */
public class SentryPolicyFileFormatFactory {

  public static SentryPolicyFileFormatter createFileFormatter(HiveAuthzConf conf) throws Exception {
    // The default formatter is org.apache.sentry.binding.hive.SentryIniPolicyFileFormatter, for ini
    // file.
    String policyFileFormatterName = conf.get(AuthzConfVars.AUTHZ_POLICY_FILE_FORMATTER.getVar());
    // load the policy file formatter class
    Constructor<?> policyFileFormatterConstructor = Class.forName(policyFileFormatterName)
        .getDeclaredConstructor();
    policyFileFormatterConstructor.setAccessible(true);
    SentryPolicyFileFormatter sentryPolicyFileFormatter = (SentryPolicyFileFormatter) policyFileFormatterConstructor
        .newInstance();
    return sentryPolicyFileFormatter;
  }
}
