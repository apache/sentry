package org.apache.sentry.tests.e2e.hive.fs;
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

public final class TestFSContants {
  private TestFSContants() {
    // no-op
  }
  public static final String SENTRY_E2E_TEST_HDFS_EXT_PATH = "test.hdfs.e2e.ext.path";
  public static final String SENTRY_E2E_TEST_HIVE_KEYTAB_LOC = "sentry.e2e.hive.keytabs.location";
  public static final String SENTRY_E2E_TEST_HDFS_ACLS_SYNCUP_SECS = "1000";
  public static final String SENTRY_E2E_TEST_DFS_ADMIN = "sentry.e2etest.dfs.admin";
  public static final String SENTRY_E2E_TEST_DFS_TYPE = "sentry.e2etest.DFSType";
  // storage default URI could be like s3a://bucketname, hdfs://nameservice, file://
  public static final String SENTRY_E2E_TEST_STORAGE_URI = "sentry.e2etest.storage.uri";
  public static final String S3A_ACCESS_KEY = "fs.s3a.access.key";
  public static final String S3A_SECRET_KEY = "fs.s3a.secret.key";
  public static final String SENTRY_E2E_TEST_SECURITY_AUTH = "hadoop.security.authentication";
}
