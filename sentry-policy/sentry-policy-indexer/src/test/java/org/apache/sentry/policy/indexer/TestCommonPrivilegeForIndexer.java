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
package org.apache.sentry.policy.indexer;

import org.apache.sentry.core.common.Model;
import org.apache.sentry.core.common.utils.KeyValue;
import org.apache.sentry.core.common.utils.SentryConstants;
import org.apache.sentry.core.model.indexer.IndexerConstants;
import org.apache.sentry.core.model.indexer.IndexerPrivilegeModel;
import org.apache.sentry.policy.common.CommonPrivilege;
import org.apache.sentry.policy.common.Privilege;
import org.junit.Before;
import org.junit.Test;

import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;

public class TestCommonPrivilegeForIndexer {

  private Model indexerPrivilegeModel;

  private static final String ALL = IndexerConstants.ALL;

  @Before
  public void prepareData() {
    indexerPrivilegeModel = IndexerPrivilegeModel.getInstance();
  }

  @Test
  public void testSimpleNoAction() throws Exception {
    CommonPrivilege indexer1 = create(new KeyValue("indexer", "ind1"));
    CommonPrivilege indexer2 = create(new KeyValue("indexer", "ind2"));
    CommonPrivilege indexer1Case = create(new KeyValue("indeXeR", "inD1"));

    assertTrue(indexer1.implies(indexer1, indexerPrivilegeModel));
    assertTrue(indexer2.implies(indexer2, indexerPrivilegeModel));
    assertTrue(indexer1.implies(indexer1Case, indexerPrivilegeModel));
    assertTrue(indexer1Case.implies(indexer1, indexerPrivilegeModel));

    assertFalse(indexer1.implies(indexer2, indexerPrivilegeModel));
    assertFalse(indexer1Case.implies(indexer2, indexerPrivilegeModel));
    assertFalse(indexer2.implies(indexer1, indexerPrivilegeModel));
    assertFalse(indexer2.implies(indexer1Case, indexerPrivilegeModel));
  }

  @Test
  public void testSimpleAction() throws Exception {
    CommonPrivilege read =
            create(new KeyValue("indexer", "ind1"), new KeyValue("action", "read"));
    CommonPrivilege write =
            create(new KeyValue("indexer", "ind1"), new KeyValue("action", "write"));
    CommonPrivilege readCase =
            create(new KeyValue("indeXeR", "iNd1"), new KeyValue("AcTiOn", "ReAd"));

    assertTrue(read.implies(read, indexerPrivilegeModel));
    assertTrue(write.implies(write, indexerPrivilegeModel));
    assertTrue(read.implies(readCase, indexerPrivilegeModel));
    assertTrue(readCase.implies(read, indexerPrivilegeModel));

    assertFalse(read.implies(write, indexerPrivilegeModel));
    assertFalse(readCase.implies(write, indexerPrivilegeModel));
    assertFalse(write.implies(read, indexerPrivilegeModel));
    assertFalse(write.implies(readCase, indexerPrivilegeModel));
  }

  @Test
  public void testRoleShorterThanRequest() throws Exception {
    CommonPrivilege indexer1 = create(new KeyValue("indexer", "ind1"));
    CommonPrivilege read =
            create(new KeyValue("indexer", "ind1"), new KeyValue("action", "read"));
    CommonPrivilege write =
            create(new KeyValue("indexer", "ind1"), new KeyValue("action", "write"));
    CommonPrivilege all =
            create(new KeyValue("indexer", "ind1"), new KeyValue("action", ALL));

    assertTrue(indexer1.implies(read, indexerPrivilegeModel));
    assertTrue(indexer1.implies(write, indexerPrivilegeModel));
    assertTrue(indexer1.implies(all, indexerPrivilegeModel));

    assertFalse(read.implies(indexer1, indexerPrivilegeModel));
    assertFalse(write.implies(indexer1, indexerPrivilegeModel));
    assertTrue(all.implies(indexer1, indexerPrivilegeModel));
  }

  @Test
  public void testIndexerAll() throws Exception {
    CommonPrivilege indexerAll = create(new KeyValue("indexer", ALL));
    CommonPrivilege indexer1 = create(new KeyValue("indexer", "ind1"));
    assertTrue(indexerAll.implies(indexer1, indexerPrivilegeModel));
    assertTrue(indexer1.implies(indexerAll, indexerPrivilegeModel));

    CommonPrivilege allWrite =
            create(new KeyValue("indexer", ALL), new KeyValue("action", "write"));
    CommonPrivilege allRead =
            create(new KeyValue("indexer", ALL), new KeyValue("action", "read"));
    CommonPrivilege ind1Write =
            create(new KeyValue("indexer", "ind1"), new KeyValue("action", "write"));
    CommonPrivilege ind1Read =
            create(new KeyValue("indexer", "ind1"), new KeyValue("action", "read"));
    assertTrue(allWrite.implies(ind1Write, indexerPrivilegeModel));
    assertTrue(allRead.implies(ind1Read, indexerPrivilegeModel));
    assertTrue(ind1Write.implies(allWrite, indexerPrivilegeModel));
    assertTrue(ind1Read.implies(allRead, indexerPrivilegeModel));
    assertFalse(allWrite.implies(ind1Read, indexerPrivilegeModel));
    assertFalse(ind1Write.implies(ind1Read, indexerPrivilegeModel));
    assertFalse(allRead.implies(ind1Write, indexerPrivilegeModel));
    assertFalse(ind1Read.implies(allWrite, indexerPrivilegeModel));
    assertFalse(allWrite.implies(allRead, indexerPrivilegeModel));
    assertFalse(allRead.implies(allWrite, indexerPrivilegeModel));
    assertFalse(ind1Write.implies(ind1Read, indexerPrivilegeModel));
    assertFalse(ind1Read.implies(ind1Write, indexerPrivilegeModel));

    // test different length paths
    assertTrue(indexerAll.implies(allWrite, indexerPrivilegeModel));
    assertTrue(indexerAll.implies(allRead, indexerPrivilegeModel));
    assertTrue(indexerAll.implies(ind1Write, indexerPrivilegeModel));
    assertTrue(indexerAll.implies(ind1Read, indexerPrivilegeModel));
    assertFalse(allWrite.implies(indexerAll, indexerPrivilegeModel));
    assertFalse(allRead.implies(indexerAll, indexerPrivilegeModel));
    assertFalse(ind1Write.implies(indexerAll, indexerPrivilegeModel));
    assertFalse(ind1Read.implies(indexerAll, indexerPrivilegeModel));
  }

  @Test
  public void testActionAll() throws Exception {
    CommonPrivilege ind1All =
            create(new KeyValue("indexer", "index1"), new KeyValue("action", ALL));
    CommonPrivilege ind1Write =
            create(new KeyValue("indexer", "index1"), new KeyValue("action", "write"));
    CommonPrivilege ind1Read =
            create(new KeyValue("indexer", "index1"), new KeyValue("action", "read"));
    assertTrue(ind1All.implies(ind1All, indexerPrivilegeModel));
    assertTrue(ind1All.implies(ind1Write, indexerPrivilegeModel));
    assertTrue(ind1All.implies(ind1Read, indexerPrivilegeModel));
    assertFalse(ind1Write.implies(ind1All, indexerPrivilegeModel));
    assertFalse(ind1Read.implies(ind1All, indexerPrivilegeModel));

    // test different lengths
    CommonPrivilege ind1 =
            create(new KeyValue("indexer", "index1"));
    assertTrue(ind1All.implies(ind1, indexerPrivilegeModel));
    assertTrue(ind1.implies(ind1All, indexerPrivilegeModel));
  }

  @Test
  public void testUnexpected() throws Exception {
    Privilege p = new Privilege() {
      @Override
      public boolean implies(Privilege p, Model model) {
        return false;
      }
    };
    CommonPrivilege indexer1 = create(new KeyValue("indexer", "index1"));
    assertFalse(indexer1.implies(null, indexerPrivilegeModel));
    assertFalse(indexer1.implies(p, indexerPrivilegeModel));
    assertFalse(indexer1.equals(null));
    assertFalse(indexer1.equals(p));
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
    System.out.println(create(SentryConstants.KV_JOINER.join("indexer", "")));
  }

  @Test(expected=IllegalArgumentException.class)
  public void testEmptyValue() throws Exception {
    System.out.println(create(SentryConstants.KV_JOINER.join("", "index1")));
  }

  @Test(expected=IllegalArgumentException.class)
  public void testEmptyPart() throws Exception {
    System.out.println(create(SentryConstants.AUTHORIZABLE_JOINER.
            join(SentryConstants.KV_JOINER.join("indexer11", "index1"), "")));
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
