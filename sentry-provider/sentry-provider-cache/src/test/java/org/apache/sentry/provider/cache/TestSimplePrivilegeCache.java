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
import org.apache.sentry.core.common.utils.KeyValue;
import org.apache.sentry.core.common.utils.SentryConstants;
import org.apache.sentry.core.model.db.Database;
import org.apache.sentry.core.model.db.Table;
import org.apache.sentry.policy.common.CommonPrivilege;
import org.junit.Test;

import org.apache.sentry.core.model.db.Server;

import static org.junit.Assert.assertEquals;

public class TestSimplePrivilegeCache {

  @Test
     public void testListPrivilegesCaseSensitivity() {
    CommonPrivilege dbSelect = create(new KeyValue("Server", "Server1"),
    new KeyValue("db", "db1"), new KeyValue("action", "SELECT"));

    SimplePrivilegeCache cache = new SimplePrivilegeCache(Sets.newHashSet(dbSelect.toString()));
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
        new KeyValue("db", "db1"), new KeyValue("table", "t1"), new KeyValue("col", "c1"), new KeyValue("action", "SELECT"));

    SimplePrivilegeCache cache = new SimplePrivilegeCache(Sets.newHashSet(t1D1Select.toString(),
        t1D2Select.toString(), t2Select.toString(), wildCardTable.toString(), allTable.toString(),
        allDatabase.toString(), colSelect.toString()));

    assertEquals(0, cache.listPrivileges(null, null, null, new Server("server1")).size());
    assertEquals(1, cache.listPrivileges(null, null, null, new Server("server1"), new Database("db1")).size());
    assertEquals(1, cache.listPrivileges(null, null, null, new Server("server1"), new Database("db2")).size());
    assertEquals(4, cache.listPrivileges(null, null, null, new Server("server1"), new Database("db1"), new Table("t1")).size());
  }

  @Test
  public void testListPrivilegesURI() {
    CommonPrivilege uri1Select = create(new KeyValue("Server", "server1"),
        new KeyValue("uri", "hdfs:///uri/path1"));
    CommonPrivilege uri2Select = create(new KeyValue("Server", "server1"),
        new KeyValue("uri", "hdfs:///uri/path2"));

    SimplePrivilegeCache cache = new SimplePrivilegeCache(Sets.newHashSet(uri1Select.toString(),
      uri2Select.toString()));

    assertEquals(2, cache.listPrivileges(null, null, null, new Server("server1")).size());
  }

  static CommonPrivilege create(KeyValue... keyValues) {
    return create(SentryConstants.AUTHORIZABLE_JOINER.join(keyValues));
  }

  static CommonPrivilege create(String s) {
    return new CommonPrivilege(s);
  }
}
