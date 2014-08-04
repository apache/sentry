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

package org.apache.sentry.provider.db.service.persistent;

import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;

import org.apache.sentry.core.model.db.AccessConstants;
import org.apache.sentry.provider.db.service.model.MSentryPrivilege;
import org.junit.Test;

public class TestSentryPrivilege {
  @Test
  public void testImpliesPrivilegePositive() throws Exception {
    // 1.test server+database+table+action
    MSentryPrivilege my = new MSentryPrivilege();
    MSentryPrivilege your = new MSentryPrivilege();
    my.setServerName("server1");
    my.setDbName("db1");
    my.setTableName("tb1");
    my.setAction(AccessConstants.SELECT);
    your.setServerName("server1");
    your.setDbName("db1");
    your.setTableName("tb1");
    your.setAction(AccessConstants.SELECT);
    assertTrue(my.implies(your));

    my.setAction(AccessConstants.ALL);
    assertTrue(my.implies(your));

    my.setTableName("");
    assertTrue(my.implies(your));

    my.setDbName("");
    assertTrue(my.implies(your));

    // 2.test server+URI+action
    my = new MSentryPrivilege();
    your = new MSentryPrivilege();
    my.setServerName("server1");
    my.setAction(AccessConstants.ALL);
    your.setServerName("server1");
    your.setAction(AccessConstants.ALL);
    my.setURI("hdfs://namenode:9000/path");
    your.setURI("hdfs://namenode:9000/path");
    assertTrue(my.implies(your));

    my.setURI("hdfs://namenode:9000/path");
    your.setURI("hdfs://namenode:9000/path/to/some/dir");
    assertTrue(my.implies(your));

    my.setURI("file:///path");
    your.setURI("file:///path");
    assertTrue(my.implies(your));

    my.setURI("file:///path");
    your.setURI("file:///path/to/some/dir");
    assertTrue(my.implies(your));
  }

  @Test
  public void testImpliesPrivilegeNegative() throws Exception {
    // 1.test server+database+table+action
    MSentryPrivilege my = new MSentryPrivilege();
    MSentryPrivilege your = new MSentryPrivilege();
    // bad action
    my.setServerName("server1");
    my.setDbName("db1");
    my.setTableName("tb1");
    my.setAction(AccessConstants.SELECT);
    your.setServerName("server1");
    your.setDbName("db1");
    your.setTableName("tb1");
    your.setAction(AccessConstants.INSERT);
    assertFalse(my.implies(your));

    // bad action
    your.setAction(AccessConstants.ALL);
    assertFalse(my.implies(your));

    // bad table
    your.setTableName("tb2");
    assertFalse(my.implies(your));

    // bad database
    your.setTableName("tb1");
    your.setDbName("db2");
    assertFalse(my.implies(your));

    // bad server
    your.setTableName("tb1");
    your.setDbName("db1");
    your.setServerName("server2");
    assertFalse(my.implies(your));

    // 2.test server+URI+action
    my = new MSentryPrivilege();
    your = new MSentryPrivilege();
    my.setServerName("server1");
    my.setAction(AccessConstants.ALL);
    your.setServerName("server2");
    your.setAction(AccessConstants.ALL);

    // relative path
    my.setURI("hdfs://namenode:9000/path");
    your.setURI("hdfs://namenode:9000/path/to/../../other");
    assertFalse(my.implies(your));
    my.setURI("file:///path");
    your.setURI("file:///path/to/../../other");
    assertFalse(my.implies(your));

    // bad uri
    my.setURI("blah");
    your.setURI("hdfs://namenode:9000/path/to/some/dir");
    assertFalse(my.implies(your));
    my.setURI("hdfs://namenode:9000/path/to/some/dir");
    your.setURI("blah");
    assertFalse(my.implies(your));

    // bad scheme
    my.setURI("hdfs://namenode:9000/path");
    your.setURI("file:///path/to/some/dir");
    assertFalse(my.implies(your));
    my.setURI("hdfs://namenode:9000/path");
    your.setURI("file://namenode:9000/path/to/some/dir");
    assertFalse(my.implies(your));

    // bad hostname
    my.setURI("hdfs://namenode1:9000/path");
    your.setURI("hdfs://namenode2:9000/path");
    assertFalse(my.implies(your));

    // bad port
    my.setURI("hdfs://namenode:9000/path");
    your.setURI("hdfs://namenode:9001/path");
    assertFalse(my.implies(your));

    // bad path
    my.setURI("hdfs://namenode:9000/path1");
    your.setURI("hdfs://namenode:9000/path2");
    assertFalse(my.implies(your));
    my.setURI("file:///path1");
    your.setURI("file:///path2");
    assertFalse(my.implies(your));

    // bad server
    your.setServerName("server2");
    my.setURI("hdfs://namenode:9000/path1");
    your.setURI("hdfs://namenode:9000/path1");
    assertFalse(my.implies(your));
  }
}
