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
package org.apache.access.provider.file;
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNull;

import org.apache.access.core.AccessURI;
import org.apache.access.core.Database;
import org.apache.access.core.Server;
import org.apache.access.core.Table;
import org.apache.access.core.View;
import org.junit.Test;

public class TestAuthorizables {

  @Test
  public void testServer() throws Exception {
    Server server = (Server)Authorizables.from("SeRvEr=server1");
    assertEquals("server1", server.getName());
  }
  @Test
  public void testDb() throws Exception {
    Database db = (Database)Authorizables.from("dB=db1");
    assertEquals("db1", db.getName());
  }
  @Test
  public void testTable() throws Exception {
    Table table = (Table)Authorizables.from("tAbLe=t1");
    assertEquals("t1", table.getName());
  }
  @Test
  public void testView() throws Exception {
    View view = (View)Authorizables.from("vIeW=v1");
    assertEquals("v1", view.getName());
  }
  @Test
  public void testURI() throws Exception {
    AccessURI uri = (AccessURI)Authorizables.from("UrI=hdfs://uri1:8200/blah");
    assertEquals("hdfs://uri1:8200/blah", uri.getName());
  }

  @Test(expected=IllegalArgumentException.class)
  public void testNoKV() throws Exception {
    System.out.println(Authorizables.from("nonsense"));
  }

  @Test(expected=IllegalArgumentException.class)
  public void testTooManyKV() throws Exception {
    System.out.println(Authorizables.from("k=v1=v2"));
  }
  @Test(expected=IllegalArgumentException.class)
  public void testEmptyKey() throws Exception {
    System.out.println(Authorizables.from("=v"));
  }
  @Test(expected=IllegalArgumentException.class)
  public void testEmptyValue() throws Exception {
    System.out.println(Authorizables.from("k="));
  }
  @Test
  public void testNotAuthorizable() throws Exception {
    assertNull(Authorizables.from("k=v"));
  }
}
