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
package org.apache.sentry.policy.db;
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNull;

import org.apache.sentry.core.model.db.AccessURI;
import org.apache.sentry.core.model.db.Database;
import org.apache.sentry.core.model.db.Server;
import org.apache.sentry.core.model.db.Table;
import org.apache.sentry.core.model.db.View;
import org.junit.Test;

public class TestDBModelAuthorizables {

  @Test
  public void testServer() throws Exception {
    Server server = (Server)DBModelAuthorizables.from("SeRvEr=server1");
    assertEquals("server1", server.getName());
  }
  @Test
  public void testDb() throws Exception {
    Database db = (Database)DBModelAuthorizables.from("dB=db1");
    assertEquals("db1", db.getName());
  }
  @Test
  public void testTable() throws Exception {
    Table table = (Table)DBModelAuthorizables.from("tAbLe=t1");
    assertEquals("t1", table.getName());
  }
  @Test
  public void testView() throws Exception {
    View view = (View)DBModelAuthorizables.from("vIeW=v1");
    assertEquals("v1", view.getName());
  }
  @Test
  public void testURI() throws Exception {
    AccessURI uri = (AccessURI)DBModelAuthorizables.from("UrI=hdfs://uri1:8200/blah");
    assertEquals("hdfs://uri1:8200/blah", uri.getName());
  }

  @Test(expected=IllegalArgumentException.class)
  public void testNoKV() throws Exception {
    System.out.println(DBModelAuthorizables.from("nonsense"));
  }

  @Test(expected=IllegalArgumentException.class)
  public void testTooManyKV() throws Exception {
    System.out.println(DBModelAuthorizables.from("k=v1=v2"));
  }
  @Test(expected=IllegalArgumentException.class)
  public void testEmptyKey() throws Exception {
    System.out.println(DBModelAuthorizables.from("=v"));
  }
  @Test(expected=IllegalArgumentException.class)
  public void testEmptyValue() throws Exception {
    System.out.println(DBModelAuthorizables.from("k="));
  }
  @Test
  public void testNotAuthorizable() throws Exception {
    assertNull(DBModelAuthorizables.from("k=v"));
  }
}
