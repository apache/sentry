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
package org.apache.sentry.policy.common;

import org.apache.sentry.core.common.Model;
import org.apache.sentry.core.common.utils.KeyValue;
import org.junit.Before;
import org.junit.Test;
import java.util.List;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;
import static junit.framework.Assert.assertFalse;

public class TestCommonPrivilege {

  private Model testModel;

  @Before
  public void prepareData() {
    testModel = new ModelForTest();
  }

  @Test
  public void testCreateCommonPrivilege() throws Exception {
    String privilegeHiveStr = "server=server1->db=db1->table=table1->column=column1->action=select";
    String privilegeSolrStr = "server=server1->collection=col1->action=update";
    String privilegeSqoopStr = "server=server1->link=link1->action=read";

    CommonPrivilege privilegeHive = new CommonPrivilege(privilegeHiveStr);
    CommonPrivilege privilegeSolr = new CommonPrivilege(privilegeSolrStr);
    CommonPrivilege privilegeSqoop = new CommonPrivilege(privilegeSqoopStr);

    List<KeyValue> keyValues = privilegeHive.getParts();
    assertEquals(5, keyValues.size());
    // test the value and the order
    assertEquals("server", keyValues.get(0).getKey());
    assertEquals("server1", keyValues.get(0).getValue());
    assertEquals("db", keyValues.get(1).getKey());
    assertEquals("db1", keyValues.get(1).getValue());
    assertEquals("table", keyValues.get(2).getKey());
    assertEquals("table1", keyValues.get(2).getValue());
    assertEquals("column", keyValues.get(3).getKey());
    assertEquals("column1", keyValues.get(3).getValue());
    assertEquals("action", keyValues.get(4).getKey());
    assertEquals("select", keyValues.get(4).getValue());

    keyValues = privilegeSolr.getParts();
    assertEquals(3, keyValues.size());
    assertEquals("server", keyValues.get(0).getKey());
    assertEquals("server1", keyValues.get(0).getValue());
    assertEquals("collection", keyValues.get(1).getKey());
    assertEquals("col1", keyValues.get(1).getValue());
    assertEquals("action", keyValues.get(2).getKey());
    assertEquals("update", keyValues.get(2).getValue());

    keyValues = privilegeSqoop.getParts();
    assertEquals(3, keyValues.size());
    assertEquals("server", keyValues.get(0).getKey());
    assertEquals("server1", keyValues.get(0).getValue());
    assertEquals("link", keyValues.get(1).getKey());
    assertEquals("link1", keyValues.get(1).getValue());
    assertEquals("action", keyValues.get(2).getKey());
    assertEquals("read", keyValues.get(2).getValue());
  }

  @Test
  public void testImplyCommonPrivilegeWithoutAction() throws Exception {

    CommonPrivilege requestPrivilege = new CommonPrivilege("server=server1->db=db1->table=table1");
    CommonPrivilege privilegForTest1 = new CommonPrivilege("server=server1->db=db1->table=table1");
    CommonPrivilege privilegForTest2 = new CommonPrivilege("server=server1->db=db1");
    CommonPrivilege privilegForTest3 = new CommonPrivilege("server=server1->db=db1->table=table2");
    CommonPrivilege privilegForTest4 = new CommonPrivilege("server=server1->db=db1->table=table1->column=col1");
    CommonPrivilege privilegForTest5 = new CommonPrivilege("server=server1->db=db1->table=table1->column=*");

    assertTrue(privilegForTest1.implies(requestPrivilege, testModel));
    assertTrue(privilegForTest2.implies(requestPrivilege, testModel));
    assertFalse(privilegForTest3.implies(requestPrivilege, testModel));
    assertFalse(privilegForTest4.implies(requestPrivilege, testModel));
    assertTrue(privilegForTest5.implies(requestPrivilege, testModel));
  }

  @Test
  public void testImplyCommonPrivilegeWithUrl() throws Exception {

    CommonPrivilege requestPrivilege = new CommonPrivilege("server=server1->uri=hdfs:///url/for/request");
    CommonPrivilege privilegForTest1 = new CommonPrivilege("server=server1->uri=hdfs:///url");
    CommonPrivilege privilegForTest2 = new CommonPrivilege("server=server1->uri=hdfs:///url/for/request");
    CommonPrivilege privilegForTest3 = new CommonPrivilege("server=server1->uri=hdfs:///url/unvalid/for/request");

    assertTrue(privilegForTest1.implies(requestPrivilege, testModel));
    assertTrue(privilegForTest2.implies(requestPrivilege, testModel));
    assertFalse(privilegForTest3.implies(requestPrivilege, testModel));
  }

  @Test
  public void testImplyCommonPrivilegeForAction() throws Exception {
    CommonPrivilege privilegForSelect = new CommonPrivilege("server=server1->db=db1->table=table1->action=select");
    CommonPrivilege privilegForInsert = new CommonPrivilege("server=server1->db=db1->table=table1->action=insert");
    CommonPrivilege privilegForAll = new CommonPrivilege("server=server1->db=db1->table=table1->action=all");

    // the privilege should imply itself
    assertTrue(privilegForSelect.implies(privilegForSelect, testModel));
    assertTrue(privilegForInsert.implies(privilegForInsert, testModel));
    assertTrue(privilegForAll.implies(privilegForAll, testModel));

    // do the imply with the different action based on operate &
    assertFalse(privilegForInsert.implies(privilegForSelect, testModel));
    assertTrue(privilegForAll.implies(privilegForSelect, testModel));

    assertFalse(privilegForSelect.implies(privilegForInsert, testModel));
    assertTrue(privilegForAll.implies(privilegForInsert, testModel));

    assertFalse(privilegForSelect.implies(privilegForAll, testModel));
    assertFalse(privilegForInsert.implies(privilegForAll, testModel));
  }

  @Test
  public void testImplyStringCaseSensitive() throws Exception {
    CommonPrivilege privileg1 = new CommonPrivilege("server=server1->db=db1->table=table1->column=col1->action=select");
    CommonPrivilege privileg2 = new CommonPrivilege("server=server1->db=db1->table=table1->column=CoL1->action=select");
    CommonPrivilege privileg3 = new CommonPrivilege("server=SERver1->db=Db1->table=TAbLe1->column=col1->action=select");
    CommonPrivilege privileg4 = new CommonPrivilege("SERVER=server1->DB=db1->TABLE=table1->COLUMN=col1->ACTION=select");

    // column is case sensitive
    assertFalse(privileg1.implies(privileg2, testModel));
    // server, db, table is case insensitive
    assertTrue(privileg1.implies(privileg3, testModel));
    // key in privilege is case insensitive
    assertTrue(privileg1.implies(privileg4, testModel));
  }
}
