/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sentry.provider.common;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;

public class ProviderConstants {

  public static final String ROLE_SEPARATOR = ",";
  public static final String AUTHORIZABLE_SEPARATOR = "->";
  public static final String KV_SEPARATOR = "=";

  public static final Splitter ROLE_SPLITTER = Splitter.on(ROLE_SEPARATOR);
  public static final Splitter AUTHORIZABLE_SPLITTER = Splitter.on(AUTHORIZABLE_SEPARATOR);
  public static final Splitter KV_SPLITTER = Splitter.on(KV_SEPARATOR);
  public static final Joiner ROLE_JOINER = Joiner.on(ROLE_SEPARATOR);
  public static final Joiner AUTHORIZABLE_JOINER = Joiner.on(AUTHORIZABLE_SEPARATOR);
  public static final Joiner KV_JOINER = Joiner.on(KV_SEPARATOR);

  // TODO change to privilege
  public static final String PRIVILEGE_NAME = "action";
  public static final String PRIVILEGE_PREFIX = (PRIVILEGE_NAME + KV_SEPARATOR).toLowerCase();
  public static final String SENTRY_ZK_JAAS_NAME = "Sentry";

  public static final String KERBEROS_MODE = "kerberos";

}
