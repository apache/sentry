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
package org.apache.sentry.policy.indexer;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;
import static org.apache.sentry.provider.file.PolicyFileConstants.AUTHORIZABLE_JOINER;
import static org.apache.sentry.provider.file.PolicyFileConstants.KV_JOINER;
import static org.apache.sentry.provider.file.PolicyFileConstants.KV_SEPARATOR;

import org.apache.sentry.core.model.indexer.IndexerConstants;
import org.apache.sentry.policy.common.Privilege;
import org.apache.sentry.provider.file.KeyValue;
import org.junit.Test;

public class TestIndexerWildcardPrivilege {

  private static final String ALL = IndexerConstants.ALL;

  @Test
  public void testSimpleNoAction() throws Exception {
    Privilege indexer1 = create(new KeyValue("indexer", "ind1"));
    Privilege indexer2 = create(new KeyValue("indexer", "ind2"));
    Privilege indexer1Case = create(new KeyValue("indeXeR", "inD1"));

    assertTrue(indexer1.implies(indexer1));
    assertTrue(indexer2.implies(indexer2));
    assertTrue(indexer1.implies(indexer1Case));
    assertTrue(indexer1Case.implies(indexer1));

    assertFalse(indexer1.implies(indexer2));
    assertFalse(indexer1Case.implies(indexer2));
    assertFalse(indexer2.implies(indexer1));
    assertFalse(indexer2.implies(indexer1Case));
  }

  @Test
  public void testSimpleAction() throws Exception {
    Privilege read =
      create(new KeyValue("indexer", "ind1"), new KeyValue("action", "read"));
    Privilege write =
      create(new KeyValue("indexer", "ind1"), new KeyValue("action", "write"));
    Privilege readCase =
      create(new KeyValue("indeXeR", "iNd1"), new KeyValue("AcTiOn", "ReAd"));

    assertTrue(read.implies(read));
    assertTrue(write.implies(write));
    assertTrue(read.implies(readCase));
    assertTrue(readCase.implies(read));

    assertFalse(read.implies(write));
    assertFalse(readCase.implies(write));
    assertFalse(write.implies(read));
    assertFalse(write.implies(readCase));
  }

  @Test
  public void testRoleShorterThanRequest() throws Exception {
    Privilege indexer1 = create(new KeyValue("indexer", "ind1"));
    Privilege read =
      create(new KeyValue("indexer", "ind1"), new KeyValue("action", "read"));
    Privilege write =
      create(new KeyValue("indexer", "ind1"), new KeyValue("action", "write"));
    Privilege all =
      create(new KeyValue("indexer", "ind1"), new KeyValue("action", ALL));

    assertTrue(indexer1.implies(read));
    assertTrue(indexer1.implies(write));
    assertTrue(indexer1.implies(all));

    assertFalse(read.implies(indexer1));
    assertFalse(write.implies(indexer1));
    assertTrue(all.implies(indexer1));
  }

  @Test
  public void testIndexerAll() throws Exception {
    Privilege indexerAll = create(new KeyValue("indexer", ALL));
    Privilege indexer1 = create(new KeyValue("indexer", "ind1"));
    assertTrue(indexerAll.implies(indexer1));
    assertTrue(indexer1.implies(indexerAll));

    Privilege allWrite =
      create(new KeyValue("indexer", ALL), new KeyValue("action", "write"));
    Privilege allRead =
      create(new KeyValue("indexer", ALL), new KeyValue("action", "read"));
    Privilege ind1Write =
      create(new KeyValue("indexer", "ind1"), new KeyValue("action", "write"));
    Privilege ind1Read =
      create(new KeyValue("indexer", "ind1"), new KeyValue("action", "read"));
    assertTrue(allWrite.implies(ind1Write));
    assertTrue(allRead.implies(ind1Read));
    assertTrue(ind1Write.implies(allWrite));
    assertTrue(ind1Read.implies(allRead));
    assertFalse(allWrite.implies(ind1Read));
    assertFalse(ind1Write.implies(ind1Read));
    assertFalse(allRead.implies(ind1Write));
    assertFalse(ind1Read.implies(allWrite));
    assertFalse(allWrite.implies(allRead));
    assertFalse(allRead.implies(allWrite));
    assertFalse(ind1Write.implies(ind1Read));
    assertFalse(ind1Read.implies(ind1Write));

    // test different length paths
    assertTrue(indexerAll.implies(allWrite));
    assertTrue(indexerAll.implies(allRead));
    assertTrue(indexerAll.implies(ind1Write));
    assertTrue(indexerAll.implies(ind1Read));
    assertFalse(allWrite.implies(indexerAll));
    assertFalse(allRead.implies(indexerAll));
    assertFalse(ind1Write.implies(indexerAll));
    assertFalse(ind1Read.implies(indexerAll));
  }

  @Test
  public void testActionAll() throws Exception {
    Privilege ind1All =
       create(new KeyValue("indexer", "index1"), new KeyValue("action", ALL));
    Privilege ind1Write =
      create(new KeyValue("indexer", "index1"), new KeyValue("action", "write"));
    Privilege ind1Read =
      create(new KeyValue("indexer", "index1"), new KeyValue("action", "read"));
    assertTrue(ind1All.implies(ind1All));
    assertTrue(ind1All.implies(ind1Write));
    assertTrue(ind1All.implies(ind1Read));
    assertFalse(ind1Write.implies(ind1All));
    assertFalse(ind1Read.implies(ind1All));

    // test different lengths
    Privilege ind1 =
       create(new KeyValue("indexer", "index1"));
    assertTrue(ind1All.implies(ind1));
    assertTrue(ind1.implies(ind1All));
  }

  @Test
  public void testUnexpected() throws Exception {
    Privilege p = new Privilege() {
      @Override
      public boolean implies(Privilege p) {
        return false;
      }
    };
    Privilege indexer1 = create(new KeyValue("indexer", "index1"));
    assertFalse(indexer1.implies(null));
    assertFalse(indexer1.implies(p));
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
    System.out.println(create(KV_JOINER.join("indexer", "")));
  }

  @Test(expected=IllegalArgumentException.class)
  public void testEmptyValue() throws Exception {
    System.out.println(create(KV_JOINER.join("", "index1")));
  }

  @Test(expected=IllegalArgumentException.class)
  public void testEmptyPart() throws Exception {
    System.out.println(create(AUTHORIZABLE_JOINER.
        join(KV_JOINER.join("indexer11", "index1"), "")));
  }

  @Test(expected=IllegalArgumentException.class)
  public void testOnlySeperators() throws Exception {
    System.out.println(create(AUTHORIZABLE_JOINER.
        join(KV_SEPARATOR, KV_SEPARATOR, KV_SEPARATOR)));
  }

  static IndexerWildcardPrivilege create(KeyValue... keyValues) {
    return create(AUTHORIZABLE_JOINER.join(keyValues));

  }
  static IndexerWildcardPrivilege create(String s) {
    return new IndexerWildcardPrivilege(s);
  }
}
