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
package org.apache.sentry.policy.sqoop;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;
import static org.apache.sentry.provider.common.ProviderConstants.AUTHORIZABLE_JOINER;
import static org.apache.sentry.provider.common.ProviderConstants.KV_JOINER;
import static org.apache.sentry.provider.common.ProviderConstants.KV_SEPARATOR;

import org.apache.sentry.core.model.sqoop.SqoopActionConstant;
import org.apache.sentry.policy.common.Privilege;
import org.apache.sentry.provider.common.KeyValue;
import org.junit.Test;

public class TestSqoopWildcardPrivilege {
  private static final Privilege SQOOP_SERVER1_ALL =
      create(new KeyValue("SERVER", "server1"), new KeyValue("action", SqoopActionConstant.ALL));
  private static final Privilege SQOOP_SERVER1_READ =
      create(new KeyValue("SERVER", "server1"), new KeyValue("action", SqoopActionConstant.READ));
  private static final Privilege SQOOP_SERVER1_WRITE =
      create(new KeyValue("SERVER", "server1"), new KeyValue("action", SqoopActionConstant.WRITE));

  private static final Privilege SQOOP_SERVER1_JOB1_ALL =
      create(new KeyValue("SERVER", "server1"), new KeyValue("JOB", "job1"), new KeyValue("action", SqoopActionConstant.ALL));
  private static final Privilege SQOOP_SERVER1_JOB1_READ =
      create(new KeyValue("SERVER", "server1"), new KeyValue("JOB", "job1"), new KeyValue("action", SqoopActionConstant.READ));
  private static final Privilege SQOOP_SERVER1_JOB1_WRITE =
      create(new KeyValue("SERVER", "server1"), new KeyValue("JOB", "job1"), new KeyValue("action", SqoopActionConstant.WRITE));

  private static final Privilege SQOOP_SERVER1_LINK1_ALL =
      create(new KeyValue("SERVER", "server1"), new KeyValue("LINK", "link1"), new KeyValue("action", SqoopActionConstant.ALL));
  private static final Privilege SQOOP_SERVER1_LINK1_READ =
      create(new KeyValue("SERVER", "server1"), new KeyValue("LINK", "link1"), new KeyValue("action", SqoopActionConstant.READ));
  private static final Privilege SQOOP_SERVER1_LINK1_WRITE =
      create(new KeyValue("SERVER", "server1"), new KeyValue("LINK", "link1"), new KeyValue("action", SqoopActionConstant.WRITE));

  private static final Privilege SQOOP_SERVER1_CONNECTOR1_ALL =
      create(new KeyValue("SERVER", "server1"), new KeyValue("CONNECTOR", "connector1"), new KeyValue("action", SqoopActionConstant.ALL));
  private static final Privilege SQOOP_SERVER1_CONNECTOR1_READ =
      create(new KeyValue("SERVER", "server1"), new KeyValue("CONNECTOR", "connector1"), new KeyValue("action", SqoopActionConstant.READ));
  private static final Privilege SQOOP_SERVER1_CONNECTOR1_WRITE =
      create(new KeyValue("SERVER", "server1"), new KeyValue("CONNECTOR", "connector1"), new KeyValue("action", SqoopActionConstant.WRITE));


  @Test
  public void testSimpleAction() throws Exception {
    //server
    assertFalse(SQOOP_SERVER1_WRITE.implies(SQOOP_SERVER1_READ));
    assertFalse(SQOOP_SERVER1_READ.implies(SQOOP_SERVER1_WRITE));
    //connector
    assertFalse(SQOOP_SERVER1_CONNECTOR1_WRITE.implies(SQOOP_SERVER1_CONNECTOR1_READ));
    assertFalse(SQOOP_SERVER1_CONNECTOR1_READ.implies(SQOOP_SERVER1_CONNECTOR1_WRITE));
    //job
    assertFalse(SQOOP_SERVER1_JOB1_READ.implies(SQOOP_SERVER1_JOB1_WRITE));
    assertFalse(SQOOP_SERVER1_JOB1_WRITE.implies(SQOOP_SERVER1_JOB1_READ));
    //link
    assertFalse(SQOOP_SERVER1_LINK1_READ.implies(SQOOP_SERVER1_LINK1_WRITE));
    assertFalse(SQOOP_SERVER1_LINK1_WRITE.implies(SQOOP_SERVER1_LINK1_READ));
  }

  @Test
  public void testShorterThanRequest() throws Exception {
    //job
    assertTrue(SQOOP_SERVER1_ALL.implies(SQOOP_SERVER1_JOB1_ALL));
    assertTrue(SQOOP_SERVER1_ALL.implies(SQOOP_SERVER1_JOB1_READ));
    assertTrue(SQOOP_SERVER1_ALL.implies(SQOOP_SERVER1_JOB1_WRITE));

    assertFalse(SQOOP_SERVER1_WRITE.implies(SQOOP_SERVER1_READ));
    assertTrue(SQOOP_SERVER1_READ.implies(SQOOP_SERVER1_JOB1_READ));
    assertTrue(SQOOP_SERVER1_WRITE.implies(SQOOP_SERVER1_JOB1_WRITE));

    //link
    assertTrue(SQOOP_SERVER1_ALL.implies(SQOOP_SERVER1_LINK1_ALL));
    assertTrue(SQOOP_SERVER1_ALL.implies(SQOOP_SERVER1_LINK1_READ));
    assertTrue(SQOOP_SERVER1_ALL.implies(SQOOP_SERVER1_LINK1_WRITE));

    assertTrue(SQOOP_SERVER1_READ.implies(SQOOP_SERVER1_LINK1_READ));
    assertTrue(SQOOP_SERVER1_WRITE.implies(SQOOP_SERVER1_LINK1_WRITE));

    //connector
    assertTrue(SQOOP_SERVER1_ALL.implies(SQOOP_SERVER1_CONNECTOR1_ALL));
    assertTrue(SQOOP_SERVER1_ALL.implies(SQOOP_SERVER1_CONNECTOR1_READ));
    assertTrue(SQOOP_SERVER1_ALL.implies(SQOOP_SERVER1_CONNECTOR1_WRITE));

    assertTrue(SQOOP_SERVER1_READ.implies(SQOOP_SERVER1_CONNECTOR1_READ));
    assertTrue(SQOOP_SERVER1_WRITE.implies(SQOOP_SERVER1_CONNECTOR1_WRITE));
  }

  @Test
  public void testActionAll() throws Exception {
    //server
    assertTrue(SQOOP_SERVER1_ALL.implies(SQOOP_SERVER1_READ));
    assertTrue(SQOOP_SERVER1_ALL.implies(SQOOP_SERVER1_WRITE));

    //job
    assertTrue(SQOOP_SERVER1_JOB1_ALL.implies(SQOOP_SERVER1_JOB1_READ));
    assertTrue(SQOOP_SERVER1_JOB1_ALL.implies(SQOOP_SERVER1_JOB1_WRITE));

    //link
    assertTrue(SQOOP_SERVER1_LINK1_ALL.implies(SQOOP_SERVER1_LINK1_READ));
    assertTrue(SQOOP_SERVER1_LINK1_ALL.implies(SQOOP_SERVER1_LINK1_WRITE));

    //connector
    assertTrue(SQOOP_SERVER1_CONNECTOR1_ALL.implies(SQOOP_SERVER1_CONNECTOR1_READ));
    assertTrue(SQOOP_SERVER1_CONNECTOR1_ALL.implies(SQOOP_SERVER1_CONNECTOR1_WRITE));
  }

  @Test
  public void testUnexpected() throws Exception {
    Privilege p = new Privilege() {
      @Override
      public boolean implies(Privilege p) {
        return false;
      }
    };
    Privilege job1 = create(new KeyValue("SERVER", "server"), new KeyValue("JOB", "job1"));
    assertFalse(job1.implies(null));
    assertFalse(job1.implies(p));
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
    System.out.println(create(KV_JOINER.join("", "server1")));
  }

  @Test(expected=IllegalArgumentException.class)
  public void testEmptyValue() throws Exception {
    System.out.println(create(KV_JOINER.join("SERVER", "")));
  }

  @Test(expected=IllegalArgumentException.class)
  public void testEmptyPart() throws Exception {
    System.out.println(create(AUTHORIZABLE_JOINER.
        join(KV_JOINER.join("SERVER", "server1"), "")));
  }

  @Test(expected=IllegalArgumentException.class)
  public void testOnlySeperators() throws Exception {
    System.out.println(create(AUTHORIZABLE_JOINER.
        join(KV_SEPARATOR, KV_SEPARATOR, KV_SEPARATOR)));
  }

  static SqoopWildcardPrivilege create(KeyValue... keyValues) {
    return create(AUTHORIZABLE_JOINER.join(keyValues));

  }
  static SqoopWildcardPrivilege create(String s) {
    return new SqoopWildcardPrivilege(s);
  }
}
