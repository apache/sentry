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
package org.apache.hadoop.hive;

import java.util.EnumSet;

import org.apache.hadoop.hive.ql.security.authorization.Privilege.PrivilegeType;

public class SentryHiveConstants {
  // TODO add INSERT
  public static final EnumSet<PrivilegeType> ALLOWED_PRIVS = EnumSet.of(PrivilegeType.ALL, PrivilegeType.SELECT);

  public static final String PRIVILEGE_NOT_SUPPORTED = "Sentry does not support privilege: ";
  public static final String COLUMN_PRIVS_NOT_SUPPORTED = "Sentry users should use views to grant privileges on columns";
  public static final String PARTITION_PRIVS_NOT_SUPPORTED = "Sentry does not support partition level authorization";
  public static final String GRANT_REVOKE_NOT_SUPPORTED_ON_OBJECT = "Sentry does not allow grant/revoke on: ";
  public static final String GRANT_REVOKE_NOT_SUPPORTED_FOR_PRINCIPAL = "Sentry does not allow privileges to be granted/revoked to/from: ";
  public static final String GRANT_OPTION_NOT_SUPPORTED = "Sentry does not allow WITH GRANT OPTION";
}
