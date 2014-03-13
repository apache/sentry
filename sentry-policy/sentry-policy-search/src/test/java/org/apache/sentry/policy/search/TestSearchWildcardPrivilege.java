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
package org.apache.sentry.policy.search;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;
import static org.apache.sentry.provider.file.PolicyFileConstants.AUTHORIZABLE_JOINER;
import static org.apache.sentry.provider.file.PolicyFileConstants.KV_JOINER;
import static org.apache.sentry.provider.file.PolicyFileConstants.KV_SEPARATOR;

import org.apache.sentry.core.model.search.SearchConstants;
import org.apache.sentry.policy.common.Privilege;
import org.apache.sentry.provider.file.KeyValue;
import org.junit.Test;

public class TestSearchWildcardPrivilege {

  private static final String ALL = SearchConstants.ALL;

  @Test
  public void testSimpleNoAction() throws Exception {
    Privilege collection1 = create(new KeyValue("collection", "coll1"));
    Privilege collection2 = create(new KeyValue("collection", "coll2"));
    Privilege collection1Case = create(new KeyValue("colleCtIon", "coLl1"));

    assertTrue(collection1.implies(collection1));
    assertTrue(collection2.implies(collection2));
    assertTrue(collection1.implies(collection1Case));
    assertTrue(collection1Case.implies(collection1));

    assertFalse(collection1.implies(collection2));
    assertFalse(collection1Case.implies(collection2));
    assertFalse(collection2.implies(collection1));
    assertFalse(collection2.implies(collection1Case));
  }

  @Test
  public void testSimpleAction() throws Exception {
    Privilege query =
      create(new KeyValue("collection", "coll1"), new KeyValue("action", "query"));
    Privilege update =
      create(new KeyValue("collection", "coll1"), new KeyValue("action", "update"));
    Privilege queryCase =
      create(new KeyValue("colleCtIon", "coLl1"), new KeyValue("AcTiOn", "QuERy"));

    assertTrue(query.implies(query));
    assertTrue(update.implies(update));
    assertTrue(query.implies(queryCase));
    assertTrue(queryCase.implies(query));

    assertFalse(query.implies(update));
    assertFalse(queryCase.implies(update));
    assertFalse(update.implies(query));
    assertFalse(update.implies(queryCase));
  }

  @Test
  public void testRoleShorterThanRequest() throws Exception {
    Privilege collection1 = create(new KeyValue("collection", "coll1"));
    Privilege query =
      create(new KeyValue("collection", "coll1"), new KeyValue("action", "query"));
    Privilege update =
      create(new KeyValue("collection", "coll1"), new KeyValue("action", "update"));
    Privilege all =
      create(new KeyValue("collection", "coll1"), new KeyValue("action", ALL));

    assertTrue(collection1.implies(query));
    assertTrue(collection1.implies(update));
    assertTrue(collection1.implies(all));

    assertFalse(query.implies(collection1));
    assertFalse(update.implies(collection1));
    assertTrue(all.implies(collection1));
  }

  @Test
  public void testCollectionAll() throws Exception {
    Privilege collectionAll = create(new KeyValue("collection", ALL));
    Privilege collection1 = create(new KeyValue("collection", "coll1"));
    assertTrue(collectionAll.implies(collection1));
    assertTrue(collection1.implies(collectionAll));

    Privilege allUpdate =
      create(new KeyValue("collection", ALL), new KeyValue("action", "update"));
    Privilege allQuery =
      create(new KeyValue("collection", ALL), new KeyValue("action", "query"));
    Privilege coll1Update =
      create(new KeyValue("collection", "coll1"), new KeyValue("action", "update"));
    Privilege coll1Query =
      create(new KeyValue("collection", "coll1"), new KeyValue("action", "query"));
    assertTrue(allUpdate.implies(coll1Update));
    assertTrue(allQuery.implies(coll1Query));
    assertTrue(coll1Update.implies(allUpdate));
    assertTrue(coll1Query.implies(allQuery));
    assertFalse(allUpdate.implies(coll1Query));
    assertFalse(coll1Update.implies(coll1Query));
    assertFalse(allQuery.implies(coll1Update));
    assertFalse(coll1Query.implies(allUpdate));
    assertFalse(allUpdate.implies(allQuery));
    assertFalse(allQuery.implies(allUpdate));
    assertFalse(coll1Update.implies(coll1Query));
    assertFalse(coll1Query.implies(coll1Update));

    // test different length paths
    assertTrue(collectionAll.implies(allUpdate));
    assertTrue(collectionAll.implies(allQuery));
    assertTrue(collectionAll.implies(coll1Update));
    assertTrue(collectionAll.implies(coll1Query));
    assertFalse(allUpdate.implies(collectionAll));
    assertFalse(allQuery.implies(collectionAll));
    assertFalse(coll1Update.implies(collectionAll));
    assertFalse(coll1Query.implies(collectionAll));
  }

  @Test
  public void testActionAll() throws Exception {
    Privilege coll1All =
       create(new KeyValue("collection", "coll1"), new KeyValue("action", ALL));
    Privilege coll1Update =
      create(new KeyValue("collection", "coll1"), new KeyValue("action", "update"));
    Privilege coll1Query =
      create(new KeyValue("collection", "coll1"), new KeyValue("action", "query"));
    assertTrue(coll1All.implies(coll1All));
    assertTrue(coll1All.implies(coll1Update));
    assertTrue(coll1All.implies(coll1Query));
    assertFalse(coll1Update.implies(coll1All));
    assertFalse(coll1Query.implies(coll1All));

    // test different lengths
    Privilege coll1 =
       create(new KeyValue("collection", "coll1"));
    assertTrue(coll1All.implies(coll1));
    assertTrue(coll1.implies(coll1All));
  }

  @Test
  public void testUnexpected() throws Exception {
    Privilege p = new Privilege() {
      @Override
      public boolean implies(Privilege p) {
        return false;
      }
    };
    Privilege collection1 = create(new KeyValue("collection", "coll1"));
    assertFalse(collection1.implies(null));
    assertFalse(collection1.implies(p));
    assertFalse(collection1.equals(null));
    assertFalse(collection1.equals(p));
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
    System.out.println(create(KV_JOINER.join("collection", "")));
  }

  @Test(expected=IllegalArgumentException.class)
  public void testEmptyValue() throws Exception {
    System.out.println(create(KV_JOINER.join("", "coll1")));
  }

  @Test(expected=IllegalArgumentException.class)
  public void testEmptyPart() throws Exception {
    System.out.println(create(AUTHORIZABLE_JOINER.
        join(KV_JOINER.join("collection1", "coll1"), "")));
  }

  @Test(expected=IllegalArgumentException.class)
  public void testOnlySeperators() throws Exception {
    System.out.println(create(AUTHORIZABLE_JOINER.
        join(KV_SEPARATOR, KV_SEPARATOR, KV_SEPARATOR)));
  }

  static SearchWildcardPrivilege create(KeyValue... keyValues) {
    return create(AUTHORIZABLE_JOINER.join(keyValues));

  }
  static SearchWildcardPrivilege create(String s) {
    return new SearchWildcardPrivilege(s);
  }
}
