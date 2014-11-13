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
package org.apache.sentry.hdfs;

public class SentryAuthorizationConstants {

  public static final String CONFIG_FILE = "hdfs-sentry.xml";

  public static final String CONFIG_PREFIX = "sentry.authorization-provider.";

  public static final String HDFS_USER_KEY = CONFIG_PREFIX + "hdfs-user";
  public static final String HDFS_USER_DEFAULT = "hive";

  public static final String HDFS_GROUP_KEY = CONFIG_PREFIX + "hdfs-group";
  public static final String HDFS_GROUP_DEFAULT = "hive";

  public static final String HDFS_PERMISSION_KEY = CONFIG_PREFIX + 
      "hdfs-permission";
  public static final long HDFS_PERMISSION_DEFAULT = 0770;

  public static final String HDFS_PATH_PREFIXES_KEY = CONFIG_PREFIX + 
      "hdfs-path-prefixes";
  public static final String[] HDFS_PATH_PREFIXES_DEFAULT = new String[0];

  public static final String CACHE_REFRESH_INTERVAL_KEY = CONFIG_PREFIX + 
      "cache-refresh-interval.ms";
  public static final int CACHE_REFRESH_INTERVAL_DEFAULT = 500;

  public static final String CACHE_STALE_THRESHOLD_KEY = CONFIG_PREFIX + 
      "cache-stale-threshold.ms";
  public static final int CACHE_STALE_THRESHOLD_DEFAULT = 60 * 1000;

  public static final String CACHE_REFRESH_RETRY_WAIT_KEY = CONFIG_PREFIX +
      "cache-refresh-retry-wait.ms";
  public static final int CACHE_REFRESH_RETRY_WAIT_DEFAULT = 30 * 1000;

  public static final String INCLUDE_HDFS_AUTHZ_AS_ACL_KEY = CONFIG_PREFIX + 
      "include-hdfs-authz-as-acl";
  public static final boolean INCLUDE_HDFS_AUTHZ_AS_ACL_DEFAULT = false;
}
