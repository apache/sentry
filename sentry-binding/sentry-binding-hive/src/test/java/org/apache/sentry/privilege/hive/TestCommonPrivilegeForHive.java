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
package org.apache.sentry.privilege.hive;

import junit.framework.Assert;
import org.apache.sentry.core.common.Model;
import org.apache.sentry.core.common.utils.KeyValue;
import org.apache.sentry.core.common.utils.PathUtils;
import org.apache.sentry.core.common.utils.SentryConstants;
import org.apache.sentry.core.model.db.AccessConstants;
import org.apache.sentry.core.model.db.HivePrivilegeModel;
import org.apache.sentry.policy.common.CommonPrivilege;
import org.apache.sentry.policy.common.Privilege;
import org.junit.Before;
import org.junit.Test;

import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;

public class TestCommonPrivilegeForHive {

  private Model hivePrivilegeModel;

  private static final String ALL = AccessConstants.ALL;

  private static final CommonPrivilege ROLE_SERVER_SERVER1_DB_ALL =
          create(new KeyValue("server", "server1"), new KeyValue("db", ALL));
  private static final CommonPrivilege ROLE_SERVER_SERVER1_DB_DB1 =
          create(new KeyValue("server", "server1"), new KeyValue("db", "db1"));
  private static final CommonPrivilege ROLE_SERVER_SERVER2_DB_ALL =
          create(new KeyValue("server", "server2"), new KeyValue("db", ALL));
  private static final CommonPrivilege ROLE_SERVER_SERVER2_DB_DB1 =
          create(new KeyValue("server", "server2"), new KeyValue("db", "db1"));
  private static final CommonPrivilege ROLE_SERVER_ALL_DB_ALL =
          create(new KeyValue("server", ALL), new KeyValue("db", ALL));
  private static final CommonPrivilege ROLE_SERVER_ALL_DB_DB1 =
          create(new KeyValue("server", ALL), new KeyValue("db", "db1"));

  private static final CommonPrivilege ROLE_SERVER_SERVER1_URI_URI1 =
          create(new KeyValue("server", "server1"), new KeyValue("uri",
                  "hdfs://namenode:8020/path/to/uri1"));
  private static final CommonPrivilege ROLE_SERVER_SERVER1_URI_URI2 =
          create(new KeyValue("server", "server1"), new KeyValue("uri",
                  "hdfs://namenode:8020/path/to/uri2/"));
  private static final CommonPrivilege ROLE_SERVER_SERVER1_URI_ALL =
          create(new KeyValue("server", "server1"), new KeyValue("uri", ALL));

  private static final CommonPrivilege ROLE_SERVER_SERVER1 =
          create(new KeyValue("server", "server1"));

  private static final CommonPrivilege REQUEST_SERVER1_DB1 =
          create(new KeyValue("server", "server1"), new KeyValue("db", "db1"));
  private static final CommonPrivilege REQUEST_SERVER2_DB1 =
          create(new KeyValue("server", "server2"), new KeyValue("db", "db1"));
  private static final CommonPrivilege REQUEST_SERVER1_DB2 =
          create(new KeyValue("server", "server1"), new KeyValue("db", "db2"));
  private static final CommonPrivilege REQUEST_SERVER2_DB2 =
          create(new KeyValue("server", "server2"), new KeyValue("db", "db2"));

  private static final CommonPrivilege REQUEST_SERVER1_URI1 =
          create(new KeyValue("server", "server1"), new KeyValue("uri",
                  "hdfs://namenode:8020/path/to/uri1/some/file"));
  private static final CommonPrivilege REQUEST_SERVER1_URI2 =
          create(new KeyValue("server", "server1"), new KeyValue("uri",
                  "hdfs://namenode:8020/path/to/uri2/some/other/file"));

  private static final CommonPrivilege REQUEST_SERVER1_OTHER =
          create(new KeyValue("server", "server2"), new KeyValue("other", "thing"));

  private static final CommonPrivilege REQUEST_SERVER1 =
          create(new KeyValue("server", "server2"));

  @Before
  public void prepareData() {
    hivePrivilegeModel = HivePrivilegeModel.getInstance();
  }

  @Test
  public void testOther() throws Exception {
    assertFalse(ROLE_SERVER_ALL_DB_ALL.implies(REQUEST_SERVER1_OTHER, hivePrivilegeModel));
    assertFalse(REQUEST_SERVER1_OTHER.implies(ROLE_SERVER_ALL_DB_ALL, hivePrivilegeModel));
  }

  @Test
  public void testRoleShorterThanRequest() throws Exception {
    assertTrue(ROLE_SERVER_SERVER1.implies(REQUEST_SERVER1_DB1, hivePrivilegeModel));
    assertTrue(ROLE_SERVER_SERVER1.implies(REQUEST_SERVER1_DB2, hivePrivilegeModel));
    assertFalse(ROLE_SERVER_SERVER1.implies(REQUEST_SERVER2_DB1, hivePrivilegeModel));
    assertFalse(ROLE_SERVER_SERVER1.implies(REQUEST_SERVER2_DB2, hivePrivilegeModel));

    assertTrue(ROLE_SERVER_ALL_DB_ALL.implies(REQUEST_SERVER1, hivePrivilegeModel));
    assertFalse(ROLE_SERVER_ALL_DB_DB1.implies(REQUEST_SERVER1, hivePrivilegeModel));
  }

  @Test
  public void testRolesAndRequests() throws Exception {
    // ROLE_SERVER_SERVER1_DB_ALL
    assertTrue(ROLE_SERVER_SERVER1_DB_ALL.implies(REQUEST_SERVER1_DB1, hivePrivilegeModel));
    assertFalse(ROLE_SERVER_SERVER1_DB_ALL.implies(REQUEST_SERVER2_DB1, hivePrivilegeModel));
    assertTrue(ROLE_SERVER_SERVER1_DB_ALL.implies(REQUEST_SERVER1_DB2, hivePrivilegeModel));
    assertFalse(ROLE_SERVER_SERVER1_DB_ALL.implies(REQUEST_SERVER2_DB2, hivePrivilegeModel));

    // test inverse
    assertTrue(REQUEST_SERVER1_DB1.implies(ROLE_SERVER_SERVER1_DB_ALL, hivePrivilegeModel));
    assertFalse(REQUEST_SERVER2_DB1.implies(ROLE_SERVER_SERVER1_DB_ALL, hivePrivilegeModel));
    assertTrue(REQUEST_SERVER1_DB2.implies(ROLE_SERVER_SERVER1_DB_ALL, hivePrivilegeModel));
    assertFalse(REQUEST_SERVER2_DB2.implies(ROLE_SERVER_SERVER1_DB_ALL, hivePrivilegeModel));

    // ROLE_SERVER_SERVER1_DB_DB1
    assertTrue(ROLE_SERVER_SERVER1_DB_DB1.implies(REQUEST_SERVER1_DB1, hivePrivilegeModel));
    assertFalse(ROLE_SERVER_SERVER1_DB_DB1.implies(REQUEST_SERVER2_DB1, hivePrivilegeModel));
    assertFalse(ROLE_SERVER_SERVER1_DB_DB1.implies(REQUEST_SERVER1_DB2, hivePrivilegeModel));
    assertFalse(ROLE_SERVER_SERVER1_DB_DB1.implies(REQUEST_SERVER2_DB2, hivePrivilegeModel));

    // test inverse
    assertTrue(REQUEST_SERVER1_DB1.implies(ROLE_SERVER_SERVER1_DB_DB1, hivePrivilegeModel));
    assertFalse(REQUEST_SERVER2_DB1.implies(ROLE_SERVER_SERVER1_DB_DB1, hivePrivilegeModel));
    assertFalse(REQUEST_SERVER1_DB2.implies(ROLE_SERVER_SERVER1_DB_DB1, hivePrivilegeModel));
    assertFalse(REQUEST_SERVER2_DB2.implies(ROLE_SERVER_SERVER1_DB_DB1, hivePrivilegeModel));

    // ROLE_SERVER_SERVER2_DB_ALL
    assertFalse(ROLE_SERVER_SERVER2_DB_ALL.implies(REQUEST_SERVER1_DB1, hivePrivilegeModel));
    assertTrue(ROLE_SERVER_SERVER2_DB_ALL.implies(REQUEST_SERVER2_DB1, hivePrivilegeModel));
    assertFalse(ROLE_SERVER_SERVER2_DB_ALL.implies(REQUEST_SERVER1_DB2, hivePrivilegeModel));
    assertTrue(ROLE_SERVER_SERVER2_DB_ALL.implies(REQUEST_SERVER2_DB2, hivePrivilegeModel));

    // test inverse
    assertFalse(REQUEST_SERVER1_DB1.implies(ROLE_SERVER_SERVER2_DB_ALL, hivePrivilegeModel));
    assertTrue(REQUEST_SERVER2_DB1.implies(ROLE_SERVER_SERVER2_DB_ALL, hivePrivilegeModel));
    assertFalse(REQUEST_SERVER1_DB2.implies(ROLE_SERVER_SERVER2_DB_ALL, hivePrivilegeModel));
    assertTrue(REQUEST_SERVER2_DB2.implies(ROLE_SERVER_SERVER2_DB_ALL, hivePrivilegeModel));

    // ROLE_SERVER_SERVER2_DB_DB1
    assertFalse(ROLE_SERVER_SERVER2_DB_DB1.implies(REQUEST_SERVER1_DB1, hivePrivilegeModel));
    assertTrue(ROLE_SERVER_SERVER2_DB_DB1.implies(REQUEST_SERVER2_DB1, hivePrivilegeModel));
    assertFalse(ROLE_SERVER_SERVER2_DB_DB1.implies(REQUEST_SERVER1_DB2, hivePrivilegeModel));
    assertFalse(ROLE_SERVER_SERVER2_DB_DB1.implies(REQUEST_SERVER2_DB2, hivePrivilegeModel));

    assertFalse(REQUEST_SERVER1_DB1.implies(ROLE_SERVER_SERVER2_DB_DB1, hivePrivilegeModel));
    assertTrue(REQUEST_SERVER2_DB1.implies(ROLE_SERVER_SERVER2_DB_DB1, hivePrivilegeModel));
    assertFalse(REQUEST_SERVER1_DB2.implies(ROLE_SERVER_SERVER2_DB_DB1, hivePrivilegeModel));
    assertFalse(REQUEST_SERVER2_DB2.implies(ROLE_SERVER_SERVER2_DB_DB1, hivePrivilegeModel));

    // ROLE_SERVER_ALL_DB_ALL
    assertTrue(ROLE_SERVER_ALL_DB_ALL.implies(REQUEST_SERVER1_DB1, hivePrivilegeModel));
    assertTrue(ROLE_SERVER_ALL_DB_ALL.implies(REQUEST_SERVER2_DB1, hivePrivilegeModel));
    assertTrue(ROLE_SERVER_ALL_DB_ALL.implies(REQUEST_SERVER1_DB2, hivePrivilegeModel));
    assertTrue(ROLE_SERVER_ALL_DB_ALL.implies(REQUEST_SERVER2_DB2, hivePrivilegeModel));

    // test inverse
    assertTrue(REQUEST_SERVER1_DB1.implies(ROLE_SERVER_ALL_DB_ALL, hivePrivilegeModel));
    assertTrue(REQUEST_SERVER2_DB1.implies(ROLE_SERVER_ALL_DB_ALL, hivePrivilegeModel));
    assertTrue(REQUEST_SERVER1_DB2.implies(ROLE_SERVER_ALL_DB_ALL, hivePrivilegeModel));
    assertTrue(REQUEST_SERVER2_DB2.implies(ROLE_SERVER_ALL_DB_ALL, hivePrivilegeModel));

    // ROLE_SERVER_ALL_DB_DB1
    assertTrue(ROLE_SERVER_ALL_DB_DB1.implies(REQUEST_SERVER1_DB1, hivePrivilegeModel));
    assertTrue(ROLE_SERVER_ALL_DB_DB1.implies(REQUEST_SERVER2_DB1, hivePrivilegeModel));
    assertFalse(ROLE_SERVER_ALL_DB_DB1.implies(REQUEST_SERVER1_DB2, hivePrivilegeModel));
    assertFalse(ROLE_SERVER_ALL_DB_DB1.implies(REQUEST_SERVER2_DB2, hivePrivilegeModel));

    // test inverse
    assertTrue(REQUEST_SERVER1_DB1.implies(ROLE_SERVER_ALL_DB_DB1, hivePrivilegeModel));
    assertTrue(REQUEST_SERVER2_DB1.implies(ROLE_SERVER_ALL_DB_DB1, hivePrivilegeModel));
    assertFalse(REQUEST_SERVER1_DB2.implies(ROLE_SERVER_ALL_DB_DB1, hivePrivilegeModel));
    assertFalse(REQUEST_SERVER2_DB2.implies(ROLE_SERVER_ALL_DB_DB1, hivePrivilegeModel));

    // uri
    assertTrue(ROLE_SERVER_SERVER1.implies(REQUEST_SERVER1_URI1, hivePrivilegeModel));
    assertTrue(ROLE_SERVER_SERVER1.implies(REQUEST_SERVER1_URI2, hivePrivilegeModel));
    assertTrue(ROLE_SERVER_SERVER1.implies(REQUEST_SERVER1_URI2, hivePrivilegeModel));
    assertTrue(ROLE_SERVER_SERVER1_URI_ALL.implies(REQUEST_SERVER1_URI1, hivePrivilegeModel));
    assertTrue(ROLE_SERVER_SERVER1_URI_ALL.implies(REQUEST_SERVER1_URI2, hivePrivilegeModel));
    assertTrue(ROLE_SERVER_SERVER1.implies(REQUEST_SERVER1_URI2, hivePrivilegeModel));
    assertTrue(ROLE_SERVER_SERVER1_URI_URI1.implies(REQUEST_SERVER1_URI1, hivePrivilegeModel));
    assertFalse(ROLE_SERVER_SERVER1_URI_URI1.implies(REQUEST_SERVER1_URI2, hivePrivilegeModel));
    assertTrue(ROLE_SERVER_SERVER1_URI_URI2.implies(REQUEST_SERVER1_URI2, hivePrivilegeModel));
    assertFalse(ROLE_SERVER_SERVER1_URI_URI2.implies(REQUEST_SERVER1_URI1, hivePrivilegeModel));
    assertFalse(REQUEST_SERVER2_DB2.implies(REQUEST_SERVER1_URI1, hivePrivilegeModel));
    assertFalse(ROLE_SERVER_ALL_DB_DB1.implies(REQUEST_SERVER1_URI1, hivePrivilegeModel));
    // test inverse
    assertTrue(REQUEST_SERVER1_URI1.implies(ROLE_SERVER_SERVER1_URI_ALL, hivePrivilegeModel));
    assertTrue(REQUEST_SERVER1_URI2.implies(ROLE_SERVER_SERVER1_URI_ALL, hivePrivilegeModel));
    assertFalse(REQUEST_SERVER1_URI1.implies(ROLE_SERVER_SERVER1, hivePrivilegeModel));
    assertFalse(REQUEST_SERVER1_URI1.implies(ROLE_SERVER_SERVER1_URI_URI1, hivePrivilegeModel));
    assertFalse(REQUEST_SERVER1_URI2.implies(ROLE_SERVER_SERVER1_URI_URI1, hivePrivilegeModel));
    assertFalse(REQUEST_SERVER1_URI2.implies(ROLE_SERVER_SERVER1_URI_URI2, hivePrivilegeModel));
    assertFalse(REQUEST_SERVER1_URI1.implies(ROLE_SERVER_SERVER1_URI_URI2, hivePrivilegeModel));
  };

  @Test
  public void testUnexpected() throws Exception {
    Privilege p = new Privilege() {
      @Override
      public boolean implies(Privilege p, Model m) {
        return false;
      }
    };
    assertFalse(ROLE_SERVER_SERVER1_DB_ALL.implies(null, hivePrivilegeModel));
    assertFalse(ROLE_SERVER_SERVER1_DB_ALL.implies(p, hivePrivilegeModel));
    assertFalse(ROLE_SERVER_SERVER1_DB_ALL.equals(null));
    assertFalse(ROLE_SERVER_SERVER1_DB_ALL.equals(p));

    Assert.assertEquals(ROLE_SERVER_SERVER1_DB_ALL.hashCode(),
            create(ROLE_SERVER_SERVER1_DB_ALL.toString()).hashCode());
  }

  @Test(expected=IllegalArgumentException.class)
  public void testNullString() throws Exception {
    System.out.println(create((String)null));
  }

  @Test(expected=IllegalArgumentException.class)
  public void testEmptyString() throws Exception {
    System.out.println(create(""));
  }

  @Test(expected=IllegalArgumentException.class)
  public void testEmptyKey() throws Exception {
    System.out.println(create(SentryConstants.KV_JOINER.join("", "db1")));
  }

  @Test(expected=IllegalArgumentException.class)
  public void testEmptyValue() throws Exception {
    System.out.println(create(SentryConstants.KV_JOINER.join("db", "")));
  }

  @Test(expected=IllegalArgumentException.class)
  public void testEmptyPart() throws Exception {
    System.out.println(create(SentryConstants.AUTHORIZABLE_JOINER.
            join(SentryConstants.KV_JOINER.join("server", "server1"), "")));
  }

  @Test(expected=IllegalArgumentException.class)
  public void testOnlySeperators() throws Exception {
    System.out.println(create(SentryConstants.AUTHORIZABLE_JOINER.
            join(SentryConstants.KV_SEPARATOR, SentryConstants.KV_SEPARATOR,
            SentryConstants.KV_SEPARATOR)));
  }

  @Test
  public void testImpliesURIPositive() throws Exception {
    assertTrue(PathUtils.impliesURI("hdfs://namenode:8020/path", "hdfs://namenode:8020/path/to/some/dir"));
    assertTrue(PathUtils.impliesURI("hdfs://namenode:8020/path", "hdfs://namenode:8020/path"));
    assertTrue(PathUtils.impliesURI("file:///path", "file:///path/to/some/dir"));
    assertTrue(PathUtils.impliesURI("file:///path", "file:///path"));
  }

  @Test
  public void testImpliesURINegative() throws Exception {
    // relative path
    assertFalse(PathUtils.impliesURI("hdfs://namenode:8020/path", "hdfs://namenode:8020/path/to/../../other"));
    assertFalse(PathUtils.impliesURI("file:///path", "file:///path/to/../../other"));
    // bad policy
    assertFalse(PathUtils.impliesURI("blah", "hdfs://namenode:8020/path/to/some/dir"));
    // bad request
    assertFalse(PathUtils.impliesURI("hdfs://namenode:8020/path", "blah"));
    // scheme
    assertFalse(PathUtils.impliesURI("hdfs://namenode:8020/path", "file:///path/to/some/dir"));
    assertFalse(PathUtils.impliesURI("hdfs://namenode:8020/path", "file://namenode:8020/path/to/some/dir"));
    // hostname
    assertFalse(PathUtils.impliesURI("hdfs://namenode1:8020/path", "hdfs://namenode2:8020/path/to/some/dir"));
    // port
    assertFalse(PathUtils.impliesURI("hdfs://namenode:8020/path", "hdfs://namenode:8021/path/to/some/dir"));
    // mangled path
    assertFalse(PathUtils.impliesURI("hdfs://namenode:8020/path", "hdfs://namenode:8020/pathFooBar"));
    // ends in /
    assertTrue(PathUtils.impliesURI("hdfs://namenode:8020/path/", "hdfs://namenode:8020/path/FooBar"));
  }

  @Test
  public void testActionHierarchy() throws Exception {
    String dbName = "db1";
    CommonPrivilege dbAll = create(new KeyValue("server", "server1"),
            new KeyValue("db", dbName), new KeyValue("action", "ALL"));

    CommonPrivilege dbSelect = create(new KeyValue("server", "server1"),
            new KeyValue("db", dbName), new KeyValue("action", "SELECT"));
    CommonPrivilege dbInsert = create(new KeyValue("server", "server1"),
            new KeyValue("db", dbName), new KeyValue("action", "INSERT"));
    CommonPrivilege dbAlter = create(new KeyValue("server", "server1"),
            new KeyValue("db", dbName), new KeyValue("action", "ALTER"));
    CommonPrivilege dbCreate = create(new KeyValue("server", "server1"),
            new KeyValue("db", dbName), new KeyValue("action", "CREATE"));
    CommonPrivilege dbDrop = create(new KeyValue("server", "server1"),
            new KeyValue("db", dbName), new KeyValue("action", "DROP"));
    CommonPrivilege dbIndex = create(new KeyValue("server", "server1"),
            new KeyValue("db", dbName), new KeyValue("action", "INDEX"));
    CommonPrivilege dbLock = create(new KeyValue("server", "server1"),
            new KeyValue("db", dbName), new KeyValue("action", "LOCK"));

    assertTrue(dbAll.implies(dbSelect, hivePrivilegeModel));
    assertTrue(dbAll.implies(dbInsert, hivePrivilegeModel));
    assertTrue(dbAll.implies(dbAlter, hivePrivilegeModel));
    assertTrue(dbAll.implies(dbCreate, hivePrivilegeModel));
    assertTrue(dbAll.implies(dbDrop, hivePrivilegeModel));
    assertTrue(dbAll.implies(dbIndex, hivePrivilegeModel));
    assertTrue(dbAll.implies(dbLock, hivePrivilegeModel));

    dbAll = create(new KeyValue("server", "server1"),
            new KeyValue("db", dbName), new KeyValue("action", "*"));

    assertTrue(dbAll.implies(dbSelect, hivePrivilegeModel));
    assertTrue(dbAll.implies(dbInsert, hivePrivilegeModel));
    assertTrue(dbAll.implies(dbAlter, hivePrivilegeModel));
    assertTrue(dbAll.implies(dbCreate, hivePrivilegeModel));
    assertTrue(dbAll.implies(dbDrop, hivePrivilegeModel));
    assertTrue(dbAll.implies(dbIndex, hivePrivilegeModel));
    assertTrue(dbAll.implies(dbLock, hivePrivilegeModel));

    dbAll = create(new KeyValue("server", "server1"),
            new KeyValue("db", dbName));

    assertTrue(dbAll.implies(dbSelect, hivePrivilegeModel));
    assertTrue(dbAll.implies(dbInsert, hivePrivilegeModel));
    assertTrue(dbAll.implies(dbAlter, hivePrivilegeModel));
    assertTrue(dbAll.implies(dbCreate, hivePrivilegeModel));
    assertTrue(dbAll.implies(dbDrop, hivePrivilegeModel));
    assertTrue(dbAll.implies(dbIndex, hivePrivilegeModel));
    assertTrue(dbAll.implies(dbLock, hivePrivilegeModel));
  }

  static CommonPrivilege create(KeyValue... keyValues) {
    return create(SentryConstants.AUTHORIZABLE_JOINER.join(keyValues));
  }

  static CommonPrivilege create(String s) {
    return new CommonPrivilege(s);
  }
}
