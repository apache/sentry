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
package org.apache.sentry.privilege.sqoop;

import org.apache.sentry.core.common.Model;
import org.apache.sentry.core.common.utils.KeyValue;
import org.apache.sentry.core.common.utils.SentryConstants;
import org.apache.sentry.core.model.sqoop.SqoopActionConstant;
import org.apache.sentry.core.model.sqoop.SqoopPrivilegeModel;
import org.apache.sentry.policy.common.CommonPrivilege;
import org.apache.sentry.policy.common.Privilege;
import org.junit.Before;
import org.junit.Test;

import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;

public class TestCommonPrivilegeForSqoop {

  private Model sqoopPrivilegeModel;

  private static final CommonPrivilege SQOOP_SERVER1_ALL =
          create(new KeyValue("SERVER", "server1"), new KeyValue("action", SqoopActionConstant.ALL));
  private static final CommonPrivilege SQOOP_SERVER1_READ =
          create(new KeyValue("SERVER", "server1"), new KeyValue("action", SqoopActionConstant.READ));
  private static final CommonPrivilege SQOOP_SERVER1_WRITE =
          create(new KeyValue("SERVER", "server1"), new KeyValue("action", SqoopActionConstant.WRITE));

  private static final CommonPrivilege SQOOP_SERVER1_JOB1_ALL =
          create(new KeyValue("SERVER", "server1"), new KeyValue("JOB", "job1"),
                  new KeyValue("action", SqoopActionConstant.ALL));
  private static final CommonPrivilege SQOOP_SERVER1_JOB1_READ =
          create(new KeyValue("SERVER", "server1"), new KeyValue("JOB", "job1"),
                  new KeyValue("action", SqoopActionConstant.READ));
  private static final CommonPrivilege SQOOP_SERVER1_JOB1_WRITE =
          create(new KeyValue("SERVER", "server1"), new KeyValue("JOB", "job1"),
                  new KeyValue("action", SqoopActionConstant.WRITE));

  private static final CommonPrivilege SQOOP_SERVER1_LINK1_ALL =
          create(new KeyValue("SERVER", "server1"), new KeyValue("LINK", "link1"),
                  new KeyValue("action", SqoopActionConstant.ALL));
  private static final CommonPrivilege SQOOP_SERVER1_LINK1_READ =
          create(new KeyValue("SERVER", "server1"), new KeyValue("LINK", "link1"),
                  new KeyValue("action", SqoopActionConstant.READ));
  private static final CommonPrivilege SQOOP_SERVER1_LINK1_WRITE =
          create(new KeyValue("SERVER", "server1"), new KeyValue("LINK", "link1"),
                  new KeyValue("action", SqoopActionConstant.WRITE));

  private static final CommonPrivilege SQOOP_SERVER1_CONNECTOR1_ALL =
          create(new KeyValue("SERVER", "server1"), new KeyValue("CONNECTOR", "connector1"),
                  new KeyValue("action", SqoopActionConstant.ALL));
  private static final CommonPrivilege SQOOP_SERVER1_CONNECTOR1_READ =
          create(new KeyValue("SERVER", "server1"), new KeyValue("CONNECTOR", "connector1"),
                  new KeyValue("action", SqoopActionConstant.READ));
  private static final CommonPrivilege SQOOP_SERVER1_CONNECTOR1_WRITE =
          create(new KeyValue("SERVER", "server1"), new KeyValue("CONNECTOR", "connector1"),
                  new KeyValue("action", SqoopActionConstant.WRITE));

  @Before
  public void prepareData() {
    sqoopPrivilegeModel = SqoopPrivilegeModel.getInstance();
  }

  @Test
  public void testSimpleAction() throws Exception {
    //server
    assertFalse(SQOOP_SERVER1_WRITE.implies(SQOOP_SERVER1_READ, sqoopPrivilegeModel));
    assertFalse(SQOOP_SERVER1_READ.implies(SQOOP_SERVER1_WRITE, sqoopPrivilegeModel));
    //connector
    assertFalse(SQOOP_SERVER1_CONNECTOR1_WRITE.implies(SQOOP_SERVER1_CONNECTOR1_READ, sqoopPrivilegeModel));
    assertFalse(SQOOP_SERVER1_CONNECTOR1_READ.implies(SQOOP_SERVER1_CONNECTOR1_WRITE, sqoopPrivilegeModel));
    //job
    assertFalse(SQOOP_SERVER1_JOB1_READ.implies(SQOOP_SERVER1_JOB1_WRITE, sqoopPrivilegeModel));
    assertFalse(SQOOP_SERVER1_JOB1_WRITE.implies(SQOOP_SERVER1_JOB1_READ, sqoopPrivilegeModel));
    //link
    assertFalse(SQOOP_SERVER1_LINK1_READ.implies(SQOOP_SERVER1_LINK1_WRITE, sqoopPrivilegeModel));
    assertFalse(SQOOP_SERVER1_LINK1_WRITE.implies(SQOOP_SERVER1_LINK1_READ, sqoopPrivilegeModel));
  }

  @Test
  public void testShorterThanRequest() throws Exception {
    //job
    assertTrue(SQOOP_SERVER1_ALL.implies(SQOOP_SERVER1_JOB1_ALL, sqoopPrivilegeModel));
    assertTrue(SQOOP_SERVER1_ALL.implies(SQOOP_SERVER1_JOB1_READ, sqoopPrivilegeModel));
    assertTrue(SQOOP_SERVER1_ALL.implies(SQOOP_SERVER1_JOB1_WRITE, sqoopPrivilegeModel));

    assertFalse(SQOOP_SERVER1_WRITE.implies(SQOOP_SERVER1_READ, sqoopPrivilegeModel));
    assertTrue(SQOOP_SERVER1_READ.implies(SQOOP_SERVER1_JOB1_READ, sqoopPrivilegeModel));
    assertTrue(SQOOP_SERVER1_WRITE.implies(SQOOP_SERVER1_JOB1_WRITE, sqoopPrivilegeModel));

    //link
    assertTrue(SQOOP_SERVER1_ALL.implies(SQOOP_SERVER1_LINK1_ALL, sqoopPrivilegeModel));
    assertTrue(SQOOP_SERVER1_ALL.implies(SQOOP_SERVER1_LINK1_READ, sqoopPrivilegeModel));
    assertTrue(SQOOP_SERVER1_ALL.implies(SQOOP_SERVER1_LINK1_WRITE, sqoopPrivilegeModel));

    assertTrue(SQOOP_SERVER1_READ.implies(SQOOP_SERVER1_LINK1_READ, sqoopPrivilegeModel));
    assertTrue(SQOOP_SERVER1_WRITE.implies(SQOOP_SERVER1_LINK1_WRITE, sqoopPrivilegeModel));

    //connector
    assertTrue(SQOOP_SERVER1_ALL.implies(SQOOP_SERVER1_CONNECTOR1_ALL, sqoopPrivilegeModel));
    assertTrue(SQOOP_SERVER1_ALL.implies(SQOOP_SERVER1_CONNECTOR1_READ, sqoopPrivilegeModel));
    assertTrue(SQOOP_SERVER1_ALL.implies(SQOOP_SERVER1_CONNECTOR1_WRITE, sqoopPrivilegeModel));

    assertTrue(SQOOP_SERVER1_READ.implies(SQOOP_SERVER1_CONNECTOR1_READ, sqoopPrivilegeModel));
    assertTrue(SQOOP_SERVER1_WRITE.implies(SQOOP_SERVER1_CONNECTOR1_WRITE, sqoopPrivilegeModel));
  }

  @Test
  public void testActionAll() throws Exception {
    //server
    assertTrue(SQOOP_SERVER1_ALL.implies(SQOOP_SERVER1_READ, sqoopPrivilegeModel));
    assertTrue(SQOOP_SERVER1_ALL.implies(SQOOP_SERVER1_WRITE, sqoopPrivilegeModel));

    //job
    assertTrue(SQOOP_SERVER1_JOB1_ALL.implies(SQOOP_SERVER1_JOB1_READ, sqoopPrivilegeModel));
    assertTrue(SQOOP_SERVER1_JOB1_ALL.implies(SQOOP_SERVER1_JOB1_WRITE, sqoopPrivilegeModel));

    //link
    assertTrue(SQOOP_SERVER1_LINK1_ALL.implies(SQOOP_SERVER1_LINK1_READ, sqoopPrivilegeModel));
    assertTrue(SQOOP_SERVER1_LINK1_ALL.implies(SQOOP_SERVER1_LINK1_WRITE, sqoopPrivilegeModel));

    //connector
    assertTrue(SQOOP_SERVER1_CONNECTOR1_ALL.implies(SQOOP_SERVER1_CONNECTOR1_READ, sqoopPrivilegeModel));
    assertTrue(SQOOP_SERVER1_CONNECTOR1_ALL.implies(SQOOP_SERVER1_CONNECTOR1_WRITE, sqoopPrivilegeModel));
  }

  @Test
  public void testUnexpected() throws Exception {
    Privilege p = new Privilege() {
      @Override
      public boolean implies(Privilege p, Model m) {
        return false;
      }
    };
    Privilege job1 = create(new KeyValue("SERVER", "server"), new KeyValue("JOB", "job1"));
    assertFalse(job1.implies(null, sqoopPrivilegeModel));
    assertFalse(job1.implies(p, sqoopPrivilegeModel));
    assertFalse(job1.equals(null));
    assertFalse(job1.equals(p));
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
    System.out.println(create(SentryConstants.KV_JOINER.join("", "server1")));
  }

  @Test(expected=IllegalArgumentException.class)
  public void testEmptyValue() throws Exception {
    System.out.println(create(SentryConstants.KV_JOINER.join("SERVER", "")));
  }

  @Test(expected=IllegalArgumentException.class)
  public void testEmptyPart() throws Exception {
    System.out.println(create(SentryConstants.AUTHORIZABLE_JOINER.
            join(SentryConstants.KV_JOINER.join("SERVER", "server1"), "")));
  }

  @Test(expected=IllegalArgumentException.class)
  public void testOnlySeperators() throws Exception {
    System.out.println(create(SentryConstants.AUTHORIZABLE_JOINER.
            join(SentryConstants.KV_SEPARATOR, SentryConstants.KV_SEPARATOR,
            SentryConstants.KV_SEPARATOR)));
  }

  static CommonPrivilege create(KeyValue... keyValues) {
    return create(SentryConstants.AUTHORIZABLE_JOINER.join(keyValues));
  }

  static CommonPrivilege create(String s) {
    return new CommonPrivilege(s);
  }
}
