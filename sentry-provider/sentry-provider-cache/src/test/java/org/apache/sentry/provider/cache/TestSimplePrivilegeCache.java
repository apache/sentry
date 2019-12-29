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

import static org.junit.Assert.assertEquals;

import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.sentry.core.common.Authorizable;
import org.apache.sentry.core.common.utils.KeyValue;
import org.apache.sentry.core.common.utils.SentryConstants;
import org.apache.sentry.core.model.db.Database;
import org.apache.sentry.core.model.db.Server;
import org.apache.sentry.core.model.db.Table;
import org.apache.sentry.policy.common.CommonPrivilege;
import org.junit.Ignore;
import org.junit.Test;

public class TestSimplePrivilegeCache {
  @Test
  public void testListPrivilegesCaseSensitivity() {
    CommonPrivilege dbSelect = create(new KeyValue("Server", "Server1"),
      new KeyValue("db", "db1"), new KeyValue("action", "SELECT"));

    SimplePrivilegeCache cache = new SimplePrivilegeCache(Sets.newHashSet(dbSelect.toString()));
    assertEquals(1, cache.listPrivileges(null, null, null).size());
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

    SimplePrivilegeCache cache = new SimplePrivilegeCache(Sets.newHashSet(t1D1Select.toString(),
      t1D2Select.toString(), t2Select.toString(), wildCardTable.toString(), allTable.toString(),
      allDatabase.toString(), colSelect.toString()));

    assertEquals(7, cache.listPrivileges(null, null, null).size());
  }

  @Test
  public void testListPrivilegesURI() {
    CommonPrivilege uri1Select = create(new KeyValue("Server", "server1"),
      new KeyValue("uri", "hdfs:///uri/path1"));
    CommonPrivilege uri2Select = create(new KeyValue("Server", "server1"),
      new KeyValue("uri", "hdfs:///uri/path2"));

    SimplePrivilegeCache cache = new SimplePrivilegeCache(Sets.newHashSet(uri1Select.toString(),
      uri2Select.toString()));

    assertEquals(2, cache.listPrivileges(null, null, null).size());
  }

  @Test
  @Ignore("This test should be run manually.")
  public void testListPrivilegesPerf() {
    int priDbCount = 1000;
    int priTableCount = 10;
    int authDbCount = 1200;
    int authTableCount = 1;
    Set<String> privileges = generatePrivilegeStrings(priDbCount, priTableCount);
    SimplePrivilegeCache cache = new SimplePrivilegeCache(privileges);
    List<Authorizable[]> authorizables = generateAuthoriables(12000, 10);
    System.out.println(
      "SimplePrivilegeCache - {privileges: db - " + priDbCount + ", table - " + priTableCount + "}, {authorizable: db - " + authDbCount + ", table - " + authTableCount + "}");

    long start = System.currentTimeMillis();
    for (Authorizable[] authorizableHierarchy : authorizables) {
      Set<String> priStrings = cache.listPrivileges(null, null, null);
      for (String priString : priStrings) {
        CommonPrivilege currPri = create(priString);
        currPri.getParts();
      }
    }
    long end = System.currentTimeMillis();

    System.out.println("SimplePrivilegeCache - total time on list string: " + (end - start) + " ms");
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
