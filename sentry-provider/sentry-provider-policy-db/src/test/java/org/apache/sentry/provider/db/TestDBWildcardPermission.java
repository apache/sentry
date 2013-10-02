/*
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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.sentry.provider.db;
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;
import static org.apache.sentry.provider.file.PolicyFileConstants.AUTHORIZABLE_JOINER;
import static org.apache.sentry.provider.file.PolicyFileConstants.KV_JOINER;
import static org.apache.sentry.provider.file.PolicyFileConstants.KV_SEPARATOR;

import org.apache.sentry.core.AccessConstants;
import org.apache.sentry.provider.file.KeyValue;
import org.apache.shiro.authz.Permission;
import org.junit.Test;

public class TestDBWildcardPermission {

  private static final String ALL = AccessConstants.ALL;

  private static final Permission ROLE_SERVER_SERVER1_DB_ALL =
      create(new KeyValue("server", "server1"), new KeyValue("db", ALL));
  private static final Permission ROLE_SERVER_SERVER1_DB_DB1 =
      create(new KeyValue("server", "server1"), new KeyValue("db", "db1"));
  private static final Permission ROLE_SERVER_SERVER2_DB_ALL =
      create(new KeyValue("server", "server2"), new KeyValue("db", ALL));
  private static final Permission ROLE_SERVER_SERVER2_DB_DB1 =
      create(new KeyValue("server", "server2"), new KeyValue("db", "db1"));
  private static final Permission ROLE_SERVER_ALL_DB_ALL =
      create(new KeyValue("server", ALL), new KeyValue("db", ALL));
  private static final Permission ROLE_SERVER_ALL_DB_DB1 =
      create(new KeyValue("server", ALL), new KeyValue("db", "db1"));

  private static final Permission ROLE_SERVER_SERVER1_URI_URI1 =
      create(new KeyValue("server", "server1"), new KeyValue("uri",
          "hdfs://namenode:8020/path/to/uri1"));
  private static final Permission ROLE_SERVER_SERVER1_URI_URI2 =
      create(new KeyValue("server", "server1"), new KeyValue("uri",
          "hdfs://namenode:8020/path/to/uri2"));
  private static final Permission ROLE_SERVER_SERVER1_URI_ALL =
      create(new KeyValue("server", "server1"), new KeyValue("uri", ALL));


  private static final Permission ROLE_SERVER_SERVER1 =
      create(new KeyValue("server", "server1"));


  private static final Permission REQUEST_SERVER1_DB1 =
      create(new KeyValue("server", "server1"), new KeyValue("db", "db1"));
  private static final Permission REQUEST_SERVER2_DB1 =
      create(new KeyValue("server", "server2"), new KeyValue("db", "db1"));
  private static final Permission REQUEST_SERVER1_DB2 =
      create(new KeyValue("server", "server1"), new KeyValue("db", "db2"));
  private static final Permission REQUEST_SERVER2_DB2 =
      create(new KeyValue("server", "server2"), new KeyValue("db", "db2"));

  private static final Permission REQUEST_SERVER1_URI1 =
      create(new KeyValue("server", "server1"), new KeyValue("uri",
          "hdfs://namenode:8020/path/to/uri1/some/file"));
  private static final Permission REQUEST_SERVER1_URI2 =
      create(new KeyValue("server", "server1"), new KeyValue("uri",
          "hdfs://namenode:8020/path/to/uri2/some/other/file"));

  private static final Permission REQUEST_SERVER1_OTHER =
      create(new KeyValue("server", "server2"), new KeyValue("other", "thing"));

  private static final Permission REQUEST_SERVER1 =
      create(new KeyValue("server", "server2"));

  @Test
  public void testOther() throws Exception {
    assertFalse(ROLE_SERVER_ALL_DB_ALL.implies(REQUEST_SERVER1_OTHER));
    assertFalse(REQUEST_SERVER1_OTHER.implies(ROLE_SERVER_ALL_DB_ALL));
  }
  @Test
  public void testRoleShorterThanRequest() throws Exception {
    assertTrue(ROLE_SERVER_SERVER1.implies(REQUEST_SERVER1_DB1));
    assertTrue(ROLE_SERVER_SERVER1.implies(REQUEST_SERVER1_DB2));
    assertFalse(ROLE_SERVER_SERVER1.implies(REQUEST_SERVER2_DB1));
    assertFalse(ROLE_SERVER_SERVER1.implies(REQUEST_SERVER2_DB2));

    assertTrue(ROLE_SERVER_ALL_DB_ALL.implies(REQUEST_SERVER1));
    assertFalse(ROLE_SERVER_ALL_DB_DB1.implies(REQUEST_SERVER1));
  }
  @Test
  public void testRolesAndRequests() throws Exception {
    // ROLE_SERVER_SERVER1_DB_ALL
    assertTrue(ROLE_SERVER_SERVER1_DB_ALL.implies(REQUEST_SERVER1_DB1));
    assertFalse(ROLE_SERVER_SERVER1_DB_ALL.implies(REQUEST_SERVER2_DB1));
    assertTrue(ROLE_SERVER_SERVER1_DB_ALL.implies(REQUEST_SERVER1_DB2));
    assertFalse(ROLE_SERVER_SERVER1_DB_ALL.implies(REQUEST_SERVER2_DB2));

    // test inverse
    assertTrue(REQUEST_SERVER1_DB1.implies(ROLE_SERVER_SERVER1_DB_ALL));
    assertFalse(REQUEST_SERVER2_DB1.implies(ROLE_SERVER_SERVER1_DB_ALL));
    assertTrue(REQUEST_SERVER1_DB2.implies(ROLE_SERVER_SERVER1_DB_ALL));
    assertFalse(REQUEST_SERVER2_DB2.implies(ROLE_SERVER_SERVER1_DB_ALL));

    // ROLE_SERVER_SERVER1_DB_DB1
    assertTrue(ROLE_SERVER_SERVER1_DB_DB1.implies(REQUEST_SERVER1_DB1));
    assertFalse(ROLE_SERVER_SERVER1_DB_DB1.implies(REQUEST_SERVER2_DB1));
    assertFalse(ROLE_SERVER_SERVER1_DB_DB1.implies(REQUEST_SERVER1_DB2));
    assertFalse(ROLE_SERVER_SERVER1_DB_DB1.implies(REQUEST_SERVER2_DB2));

    // test inverse
    assertTrue(REQUEST_SERVER1_DB1.implies(ROLE_SERVER_SERVER1_DB_DB1));
    assertFalse(REQUEST_SERVER2_DB1.implies(ROLE_SERVER_SERVER1_DB_DB1));
    assertFalse(REQUEST_SERVER1_DB2.implies(ROLE_SERVER_SERVER1_DB_DB1));
    assertFalse(REQUEST_SERVER2_DB2.implies(ROLE_SERVER_SERVER1_DB_DB1));

    // ROLE_SERVER_SERVER2_DB_ALL
    assertFalse(ROLE_SERVER_SERVER2_DB_ALL.implies(REQUEST_SERVER1_DB1));
    assertTrue(ROLE_SERVER_SERVER2_DB_ALL.implies(REQUEST_SERVER2_DB1));
    assertFalse(ROLE_SERVER_SERVER2_DB_ALL.implies(REQUEST_SERVER1_DB2));
    assertTrue(ROLE_SERVER_SERVER2_DB_ALL.implies(REQUEST_SERVER2_DB2));

    // test inverse
    assertFalse(REQUEST_SERVER1_DB1.implies(ROLE_SERVER_SERVER2_DB_ALL));
    assertTrue(REQUEST_SERVER2_DB1.implies(ROLE_SERVER_SERVER2_DB_ALL));
    assertFalse(REQUEST_SERVER1_DB2.implies(ROLE_SERVER_SERVER2_DB_ALL));
    assertTrue(REQUEST_SERVER2_DB2.implies(ROLE_SERVER_SERVER2_DB_ALL));

    // ROLE_SERVER_SERVER2_DB_DB1
    assertFalse(ROLE_SERVER_SERVER2_DB_DB1.implies(REQUEST_SERVER1_DB1));
    assertTrue(ROLE_SERVER_SERVER2_DB_DB1.implies(REQUEST_SERVER2_DB1));
    assertFalse(ROLE_SERVER_SERVER2_DB_DB1.implies(REQUEST_SERVER1_DB2));
    assertFalse(ROLE_SERVER_SERVER2_DB_DB1.implies(REQUEST_SERVER2_DB2));

    assertFalse(REQUEST_SERVER1_DB1.implies(ROLE_SERVER_SERVER2_DB_DB1));
    assertTrue(REQUEST_SERVER2_DB1.implies(ROLE_SERVER_SERVER2_DB_DB1));
    assertFalse(REQUEST_SERVER1_DB2.implies(ROLE_SERVER_SERVER2_DB_DB1));
    assertFalse(REQUEST_SERVER2_DB2.implies(ROLE_SERVER_SERVER2_DB_DB1));

    // ROLE_SERVER_ALL_DB_ALL
    assertTrue(ROLE_SERVER_ALL_DB_ALL.implies(REQUEST_SERVER1_DB1));
    assertTrue(ROLE_SERVER_ALL_DB_ALL.implies(REQUEST_SERVER2_DB1));
    assertTrue(ROLE_SERVER_ALL_DB_ALL.implies(REQUEST_SERVER1_DB2));
    assertTrue(ROLE_SERVER_ALL_DB_ALL.implies(REQUEST_SERVER2_DB2));

    // test inverse
    assertTrue(REQUEST_SERVER1_DB1.implies(ROLE_SERVER_ALL_DB_ALL));
    assertTrue(REQUEST_SERVER2_DB1.implies(ROLE_SERVER_ALL_DB_ALL));
    assertTrue(REQUEST_SERVER1_DB2.implies(ROLE_SERVER_ALL_DB_ALL));
    assertTrue(REQUEST_SERVER2_DB2.implies(ROLE_SERVER_ALL_DB_ALL));

    // ROLE_SERVER_ALL_DB_DB1
    assertTrue(ROLE_SERVER_ALL_DB_DB1.implies(REQUEST_SERVER1_DB1));
    assertTrue(ROLE_SERVER_ALL_DB_DB1.implies(REQUEST_SERVER2_DB1));
    assertFalse(ROLE_SERVER_ALL_DB_DB1.implies(REQUEST_SERVER1_DB2));
    assertFalse(ROLE_SERVER_ALL_DB_DB1.implies(REQUEST_SERVER2_DB2));

    // test inverse
    assertTrue(REQUEST_SERVER1_DB1.implies(ROLE_SERVER_ALL_DB_DB1));
    assertTrue(REQUEST_SERVER2_DB1.implies(ROLE_SERVER_ALL_DB_DB1));
    assertFalse(REQUEST_SERVER1_DB2.implies(ROLE_SERVER_ALL_DB_DB1));
    assertFalse(REQUEST_SERVER2_DB2.implies(ROLE_SERVER_ALL_DB_DB1));

    // uri
    assertTrue(ROLE_SERVER_SERVER1.implies(REQUEST_SERVER1_URI1));
    assertTrue(ROLE_SERVER_SERVER1.implies(REQUEST_SERVER1_URI2));
    assertTrue(ROLE_SERVER_SERVER1.implies(REQUEST_SERVER1_URI2));
    assertTrue(ROLE_SERVER_SERVER1_URI_ALL.implies(REQUEST_SERVER1_URI1));
    assertTrue(ROLE_SERVER_SERVER1_URI_ALL.implies(REQUEST_SERVER1_URI2));
    assertTrue(ROLE_SERVER_SERVER1.implies(REQUEST_SERVER1_URI2));
    assertTrue(ROLE_SERVER_SERVER1_URI_URI1.implies(REQUEST_SERVER1_URI1));
    assertFalse(ROLE_SERVER_SERVER1_URI_URI1.implies(REQUEST_SERVER1_URI2));
    assertTrue(ROLE_SERVER_SERVER1_URI_URI2.implies(REQUEST_SERVER1_URI2));
    assertFalse(ROLE_SERVER_SERVER1_URI_URI2.implies(REQUEST_SERVER1_URI1));
    assertFalse(REQUEST_SERVER2_DB2.implies(REQUEST_SERVER1_URI1));
    assertFalse(ROLE_SERVER_ALL_DB_DB1.implies(REQUEST_SERVER1_URI1));
    // test inverse
    assertTrue(REQUEST_SERVER1_URI1.implies(ROLE_SERVER_SERVER1_URI_ALL));
    assertTrue(REQUEST_SERVER1_URI2.implies(ROLE_SERVER_SERVER1_URI_ALL));
    assertFalse(REQUEST_SERVER1_URI1.implies(ROLE_SERVER_SERVER1));
    assertFalse(REQUEST_SERVER1_URI1.implies(ROLE_SERVER_SERVER1_URI_URI1));
    assertFalse(REQUEST_SERVER1_URI2.implies(ROLE_SERVER_SERVER1_URI_URI1));
    assertFalse(REQUEST_SERVER1_URI2.implies(ROLE_SERVER_SERVER1_URI_URI2));
    assertFalse(REQUEST_SERVER1_URI1.implies(ROLE_SERVER_SERVER1_URI_URI2));
  };
  @Test
  public void testUnexpected() throws Exception {
    Permission p = new Permission() {
      @Override
      public boolean implies(Permission p) {
        return false;
      }
    };
    assertFalse(ROLE_SERVER_SERVER1_DB_ALL.implies(null));
    assertFalse(ROLE_SERVER_SERVER1_DB_ALL.implies(p));
    assertFalse(ROLE_SERVER_SERVER1_DB_ALL.equals(null));
    assertFalse(ROLE_SERVER_SERVER1_DB_ALL.equals(p));

    assertEquals(ROLE_SERVER_SERVER1_DB_ALL.hashCode(),
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
    System.out.println(create(KV_JOINER.join("", "db1")));
  }
  @Test(expected=IllegalArgumentException.class)
  public void testEmptyValue() throws Exception {
    System.out.println(create(KV_JOINER.join("db", "")));
  }
  @Test(expected=IllegalArgumentException.class)
  public void testEmptyPart() throws Exception {
    System.out.println(create(AUTHORIZABLE_JOINER.
        join(KV_JOINER.join("server", "server1"), "")));
  }
  @Test(expected=IllegalArgumentException.class)
  public void testOnlySeperators() throws Exception {
    System.out.println(create(AUTHORIZABLE_JOINER.
        join(KV_SEPARATOR, KV_SEPARATOR, KV_SEPARATOR)));
  }
  @Test
  public void testImpliesURIPositive() throws Exception {
    assertTrue(DBWildcardPermission.impliesURI("hdfs://namenode:8020/path",
        "hdfs://namenode:8020/path/to/some/dir"));
    assertTrue(DBWildcardPermission.impliesURI("hdfs://namenode:8020/path",
        "hdfs://namenode:8020/path"));
    assertTrue(DBWildcardPermission.impliesURI("file:///path",
        "file:///path/to/some/dir"));
    assertTrue(DBWildcardPermission.impliesURI("file:///path",
        "file:///path"));
  }
  @Test
  public void testImpliesURINegative() throws Exception {
    // relative path
    assertFalse(DBWildcardPermission.impliesURI("hdfs://namenode:8020/path",
        "hdfs://namenode:8020/path/to/../../other"));
    assertFalse(DBWildcardPermission.impliesURI("file:///path",
        "file:///path/to/../../other"));
    // bad policy
    assertFalse(DBWildcardPermission.impliesURI("blah",
        "hdfs://namenode:8020/path/to/some/dir"));
    // bad request
    assertFalse(DBWildcardPermission.impliesURI("hdfs://namenode:8020/path",
        "blah"));
    // scheme
    assertFalse(DBWildcardPermission.impliesURI("hdfs://namenode:8020/path",
        "file:///path/to/some/dir"));
    assertFalse(DBWildcardPermission.impliesURI("hdfs://namenode:8020/path",
        "file://namenode:8020/path/to/some/dir"));
    // hostname
    assertFalse(DBWildcardPermission.impliesURI("hdfs://namenode1:8020/path",
        "hdfs://namenode2:8020/path/to/some/dir"));
    // port
    assertFalse(DBWildcardPermission.impliesURI("hdfs://namenode:8020/path",
        "hdfs://namenode:8021/path/to/some/dir"));
    // mangled path
    assertFalse(DBWildcardPermission.impliesURI("hdfs://namenode:8020/path",
        "hdfs://namenode:8020/pathFooBar"));
  }
  static DBWildcardPermission create(KeyValue... keyValues) {
    return create(AUTHORIZABLE_JOINER.join(keyValues));

  }
  static DBWildcardPermission create(String s) {
    return new DBWildcardPermission(s);
  }
}
