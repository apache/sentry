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
package org.apache.sentry.provider.cache;

import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.sentry.core.common.Authorizable;
import org.apache.sentry.core.common.utils.KeyValue;
import org.apache.sentry.core.common.utils.SentryConstants;
import org.apache.sentry.core.model.db.Column;
import org.apache.sentry.core.model.db.Database;
import org.apache.sentry.core.model.db.Table;
import org.apache.sentry.policy.common.CommonPrivilege;
import org.apache.sentry.policy.common.Privilege;
import org.junit.Ignore;
import org.junit.Test;

import org.apache.sentry.core.model.db.Server;

import static org.junit.Assert.assertEquals;

public class TestSimpleFilteredPrivilegeCache {

  @Test
  public void testListPrivilegesCaseSensitivity() {
    CommonPrivilege dbSelect = create(new KeyValue("Server", "Server1"),
    new KeyValue("db", "db1"), new KeyValue("action", "SELECT"));

    SimpleFilteredPrivilegeCache cache = new SimpleFilteredPrivilegeCache(Sets.newHashSet(dbSelect.toString()), null);
    assertEquals(1, cache.listPrivileges(null, null, null,
        new Server("server1"), new Database("db1")).size());
  }

  @Test
  public void testListPrivilegesWildCard() {
    CommonPrivilege t1D1Select = create(new KeyValue("Server", "server1"),
        new KeyValue("db", "db1"), new KeyValue("table", "t1"), new KeyValue("action", "SELECT"));
    CommonPrivilege t1D2Select = create(new KeyValue("Server", "server1"),
        new KeyValue("db", "db2"), new KeyValue("table", "t1"), new KeyValue("action", "SELECT"));
    CommonPrivilege t2Select = create(new KeyValue("Server", "server1"),
        new KeyValue("db", "db1"), new KeyValue("table", "t2"), new KeyValue("action", "SELECT"));
    CommonPrivilege wildCardTable = create(new KeyValue("Server", "server1"),
        new KeyValue("db", "db1"), new KeyValue("table", "*"), new KeyValue("action", "SELECT"));
    CommonPrivilege allTable = create(new KeyValue("Server", "server1"),
        new KeyValue("db", "db1"), new KeyValue("table", "ALL"), new KeyValue("action", "SELECT"));
    CommonPrivilege allDatabase = create(new KeyValue("Server", "server1"),
        new KeyValue("db", "*"));
    CommonPrivilege colSelect = create(new KeyValue("Server", "server1"),
        new KeyValue("db", "db1"), new KeyValue("table", "t1"), new KeyValue("column", "c1"), new KeyValue("action", "SELECT"));

    SimpleFilteredPrivilegeCache cache = new SimpleFilteredPrivilegeCache(Sets.newHashSet(t1D1Select.toString(),
        t1D2Select.toString(), t2Select.toString(), wildCardTable.toString(), allTable.toString(),
        allDatabase.toString(), colSelect.toString()), null);

    assertEquals(0, cache.listPrivileges(null, null, null, new Server("server1")).size());
    assertEquals(1, cache.listPrivileges(null, null, null, new Server("server1"), new Database("db1")).size());
    assertEquals(1, cache.listPrivileges(null, null, null, new Server("server1"), new Database("db2")).size());
    assertEquals(4, cache.listPrivileges(null, null, null, new Server("server1"), new Database("db1"), new Table("t1")).size());
    assertEquals(5, cache.listPrivileges(null, null, null, new Server("server1"), new Database("db1"), new Table("*")).size());
    assertEquals(5, cache.listPrivileges(null, null, null, new Server("server1"), new Database("db1"), new Table("t1"), new Column("*")).size());
    assertEquals(6, cache.listPrivileges(null, null, null, new Server("server1"), new Database("db1"), new Table("*"), new Column("*")).size());
    assertEquals(2, cache.listPrivileges(null, null, null, new Server("server1"), new Database("db2"), new Table("t1"), new Column("*")).size());
  }

  @Test
  public void testListPrivilegeObjectsWildCard() {
    CommonPrivilege t1D1Select = create(new KeyValue("Server", "server1"),
      new KeyValue("db", "db1"), new KeyValue("table", "t1"), new KeyValue("action", "SELECT"));
    CommonPrivilege t1D2Select = create(new KeyValue("Server", "server1"),
      new KeyValue("db", "db2"), new KeyValue("table", "t1"), new KeyValue("action", "SELECT"));
    CommonPrivilege t2Select = create(new KeyValue("Server", "server1"),
      new KeyValue("db", "db1"), new KeyValue("table", "t2"), new KeyValue("action", "SELECT"));
    CommonPrivilege wildCardTable = create(new KeyValue("Server", "server1"),
      new KeyValue("db", "db1"), new KeyValue("table", "*"), new KeyValue("action", "SELECT"));
    CommonPrivilege allTable = create(new KeyValue("Server", "server1"),
      new KeyValue("db", "db1"), new KeyValue("table", "ALL"), new KeyValue("action", "SELECT"));
    CommonPrivilege allDatabase = create(new KeyValue("Server", "server1"),
      new KeyValue("db", "*"));
    CommonPrivilege colSelect = create(new KeyValue("Server", "server1"),
      new KeyValue("db", "db1"), new KeyValue("table", "t1"), new KeyValue("column", "c1"), new KeyValue("action", "SELECT"));

    SimpleFilteredPrivilegeCache cache = new SimpleFilteredPrivilegeCache(Sets.newHashSet(t1D1Select.toString(),
      t1D2Select.toString(), t2Select.toString(), wildCardTable.toString(), allTable.toString(),
      allDatabase.toString(), colSelect.toString()), null);

    assertEquals(0, cache.listPrivilegeObjects(null, null, null, new Server("server1")).size());
    assertEquals(1, cache.listPrivilegeObjects(null, null, null, new Server("server1"), new Database("db1")).size());
    assertEquals(1, cache.listPrivilegeObjects(null, null, null, new Server("server1"), new Database("db2")).size());
    assertEquals(4, cache.listPrivilegeObjects(null, null, null, new Server("server1"), new Database("db1"), new Table("t1")).size());
    assertEquals(5, cache.listPrivilegeObjects(null, null, null, new Server("server1"), new Database("db1"), new Table("*")).size());
    assertEquals(5, cache.listPrivilegeObjects(null, null, null, new Server("server1"), new Database("db1"), new Table("t1"), new Column("*")).size());
    assertEquals(6, cache.listPrivilegeObjects(null, null, null, new Server("server1"), new Database("db1"), new Table("*"), new Column("*")).size());
    assertEquals(2, cache.listPrivilegeObjects(null, null, null, new Server("server1"), new Database("db2"), new Table("t1"), new Column("*")).size());
  }

  @Test
  public void testListPrivilegesURI() {
    CommonPrivilege uri1Select = create(new KeyValue("Server", "server1"),
        new KeyValue("uri", "hdfs:///uri/path1"));
    CommonPrivilege uri2Select = create(new KeyValue("Server", "server1"),
        new KeyValue("uri", "hdfs:///uri/path2"));

    SimpleFilteredPrivilegeCache cache = new SimpleFilteredPrivilegeCache(Sets.newHashSet(uri1Select.toString(),
      uri2Select.toString()), null);

    assertEquals(2, cache.listPrivileges(null, null, null, new Server("server1")).size());
  }

  @Test
  @Ignore("This test should be run manually.")
  public void testListPrivilegesPerf() {

    Set<String> privileges = generatePrivilegeStrings(1000, 10);
    SimpleFilteredPrivilegeCache cache = new SimpleFilteredPrivilegeCache(privileges, null);
    List<Authorizable[]> authorizables = generateAuthoriables(12000, 10);

    long start = System.currentTimeMillis();
    for (Authorizable[] authorizableHierarchy : authorizables) {
      Set<String> priStrings = cache.listPrivileges(null, null, null, authorizableHierarchy);
      for (String priString : priStrings) {
        CommonPrivilege currPri = create(priString);
        currPri.getParts();
      }
    }
    long end = System.currentTimeMillis();

    System.out.println("SimpleFilteredPrivilegeCache - total time on list string: " + (end - start) + " ms");
  }

  @Test
  @Ignore("This test should be run manually.")
  public void testListPrivilegeObjectsPerf() {

    Set<String> privileges = generatePrivilegeStrings(1000, 10);
    SimpleFilteredPrivilegeCache cache = new SimpleFilteredPrivilegeCache(privileges, null);
    List<Authorizable[]> authorizables = generateAuthoriables(12000, 10);

    long start = System.currentTimeMillis();
    for (Authorizable[] authorizableHierarchy : authorizables) {
      Set<Privilege> privilegeSet = cache.listPrivilegeObjects(null, null, null, authorizableHierarchy);
      for (Privilege currPri : privilegeSet) {
        currPri.getParts();
      }
    }
    long end = System.currentTimeMillis();

    System.out.println("SimpleFilteredPrivilegeCache - total time on list obj: " + (end - start) + " ms");
  }

  Set<String> generatePrivilegeStrings(int dbCount, int tableCount) {
    Set<String> priStrings = new HashSet<>();
    for (int i = 0; i < dbCount; i ++) {
      for (int j = 0; j < tableCount; j ++) {
        String priString = "Server=server1->Database=db" + i + "->Table=table" + j;
        priStrings.add(priString);
      }
    }

    return priStrings;
  }

  List<Authorizable[]> generateAuthoriables(int dbCount, int tableCount) {
    List<Authorizable[]> authorizables = new ArrayList<>();

    for (int i = 0; i < dbCount; i ++) {
      for (int j = 0; j < tableCount; j ++) {
        Authorizable[] authorizable = new Authorizable[3];
        authorizable[0] = new Server("server1");
        authorizable[1] = new Database("db" + i);
        authorizable[2] = new Table("table" + j);

        authorizables.add(authorizable);
      }
    }

    return authorizables;
  }


  static CommonPrivilege create(KeyValue... keyValues) {
    return create(SentryConstants.AUTHORIZABLE_JOINER.join(keyValues));
  }

  static CommonPrivilege create(String s) {
    return new CommonPrivilege(s);
  }
}
