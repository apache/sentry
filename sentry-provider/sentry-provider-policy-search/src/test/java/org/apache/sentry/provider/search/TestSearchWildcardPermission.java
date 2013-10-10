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
package org.apache.sentry.provider.search;
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;
import static org.apache.sentry.provider.file.PolicyFileConstants.AUTHORIZABLE_JOINER;
import static org.apache.sentry.provider.file.PolicyFileConstants.KV_JOINER;
import static org.apache.sentry.provider.file.PolicyFileConstants.KV_SEPARATOR;

import org.apache.sentry.core.model.search.SearchConstants;
import org.apache.sentry.provider.file.KeyValue;
import org.apache.shiro.authz.Permission;
import org.junit.Test;

public class TestSearchWildcardPermission {

  private static final String ALL = SearchConstants.ALL;

  @Test
  public void testSimpleNoAction() throws Exception {
    Permission collection1 = create(new KeyValue("collection", "coll1"));
    Permission collection2 = create(new KeyValue("collection", "coll2"));
    Permission collection1Case = create(new KeyValue("colleCtIon", "coLl1"));

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
    Permission query =
      create(new KeyValue("collection", "coll1"), new KeyValue("action", "query"));
    Permission update =
      create(new KeyValue("collection", "coll1"), new KeyValue("action", "update"));
    Permission queryCase =
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
    Permission collection1 = create(new KeyValue("collection", "coll1"));
    Permission query =
      create(new KeyValue("collection", "coll1"), new KeyValue("action", "query"));
    Permission update =
      create(new KeyValue("collection", "coll1"), new KeyValue("action", "update"));
    Permission all =
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
    Permission collectionAll = create(new KeyValue("collection", ALL));
    Permission collection1 = create(new KeyValue("collection", "coll1"));
    assertTrue(collectionAll.implies(collection1));
    assertTrue(collection1.implies(collectionAll));

    Permission allUpdate =
      create(new KeyValue("collection", ALL), new KeyValue("action", "update"));
    Permission allQuery =
      create(new KeyValue("collection", ALL), new KeyValue("action", "query"));
    Permission coll1Update =
      create(new KeyValue("collection", "coll1"), new KeyValue("action", "update"));
    Permission coll1Query =
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
    Permission coll1All =
       create(new KeyValue("collection", "coll1"), new KeyValue("action", ALL));
    Permission coll1Update =
      create(new KeyValue("collection", "coll1"), new KeyValue("action", "update"));
    Permission coll1Query =
      create(new KeyValue("collection", "coll1"), new KeyValue("action", "query"));
    assertTrue(coll1All.implies(coll1All));
    assertTrue(coll1All.implies(coll1Update));
    assertTrue(coll1All.implies(coll1Query));
    assertFalse(coll1Update.implies(coll1All));
    assertFalse(coll1Query.implies(coll1All));

    // test different lengths
    Permission coll1 =
       create(new KeyValue("collection", "coll1"));
    assertTrue(coll1All.implies(coll1));
    assertTrue(coll1.implies(coll1All));
  }

  @Test
  public void testUnexpected() throws Exception {
    Permission p = new Permission() {
      @Override
      public boolean implies(Permission p) {
        return false;
      }
    };
    Permission collection1 = create(new KeyValue("collection", "coll1"));
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

  static SearchWildcardPermission create(KeyValue... keyValues) {
    return create(AUTHORIZABLE_JOINER.join(keyValues));

  }
  static SearchWildcardPermission create(String s) {
    return new SearchWildcardPermission(s);
  }
}
