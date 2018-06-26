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
package org.apache.sentry.core.common.utils;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;

public class SentryConstants {

  public static final String ROLE_SEPARATOR = ",";
  public static final String AUTHORIZABLE_SEPARATOR = "->";
  public static final String KV_SEPARATOR = "=";

  public static final Splitter ROLE_SPLITTER = Splitter.on(ROLE_SEPARATOR);
  public static final Splitter AUTHORIZABLE_SPLITTER = Splitter.on(AUTHORIZABLE_SEPARATOR);
  public static final Splitter KV_SPLITTER = Splitter.on(KV_SEPARATOR);
  public static final Joiner ROLE_JOINER = Joiner.on(ROLE_SEPARATOR);
  public static final Joiner AUTHORIZABLE_JOINER = Joiner.on(AUTHORIZABLE_SEPARATOR);
  public static final Joiner KV_JOINER = Joiner.on(KV_SEPARATOR);

  public static final String PRIVILEGE_NAME = "action";
  public static final String PRIVILEGE_PREFIX = (PRIVILEGE_NAME + KV_SEPARATOR).toLowerCase();
  public static final String PRIVILEGE_WILDCARD_VALUE = "*";

  public static final String RESOURCE_WILDCARD_VALUE = "*";
  public static final String RESOURCE_WILDCARD_VALUE_ALL = "ALL";
  public static final String RESOURCE_WILDCARD_VALUE_SOME = "+";
  public static final String ACCESS_ALLOW_URI_PER_DB_POLICYFILE = "sentry.allow.uri.db.policyfile";

  public static final String SENTRY_ZK_JAAS_NAME = "Sentry";

  public static final String KERBEROS_MODE = "kerberos";

  // Sentry Store constants

  public static final String NULL_COL = "__NULL__";
  public static final int INDEX_GROUP_ROLES_MAP = 0;
  public static final int INDEX_USER_ROLES_MAP = 1;

  // String constants for field names
  public static final String SERVER_NAME = "serverName";
  public static final String DB_NAME = "dbName";
  public static final String TABLE_NAME = "tableName";
  public static final String COLUMN_NAME = "columnName";
  public static final String ACTION = "action";
  public static final String URI = "URI";
  public static final String GRANT_OPTION = "grantOption";
  public static final String ROLE_NAME = "roleName";

  // Initial change ID for permission/path change. Auto increment
  // is starting from 1.
  public static final long INIT_CHANGE_ID = 1L;

  public static final long EMPTY_CHANGE_ID = 0L;

  public static final long EMPTY_NOTIFICATION_ID = 0L;

  // Representation for empty HMS snapshots not found on MAuthzPathsSnapshotId
  public static final long EMPTY_PATHS_SNAPSHOT_ID = 0L;

}
