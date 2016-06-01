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
package org.apache.sentry.privilege.solr;

import org.apache.sentry.core.common.Model;
import org.apache.sentry.core.common.utils.KeyValue;
import org.apache.sentry.core.common.utils.SentryConstants;
import org.apache.sentry.core.model.search.SearchConstants;
import org.apache.sentry.core.model.search.SearchPrivilegeModel;
import org.apache.sentry.policy.common.CommonPrivilege;
import org.apache.sentry.policy.common.Privilege;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;

public class TestCommonPrivilegeForSearch {

  private Model searchPrivilegeModel;

  private static final String ALL = SearchConstants.ALL;

  @Before
  public void prepareData() {
    searchPrivilegeModel = SearchPrivilegeModel.getInstance();
  }

  @Test
  public void testSimpleNoAction() throws Exception {
    CommonPrivilege collection1 = create(new KeyValue("collection", "coll1"));
    CommonPrivilege collection2 = create(new KeyValue("collection", "coll2"));
    CommonPrivilege collection1Case = create(new KeyValue("colleCtIon", "coLl1"));

    assertTrue(collection1.implies(collection1, searchPrivilegeModel));
    assertTrue(collection2.implies(collection2, searchPrivilegeModel));
    assertTrue(collection1.implies(collection1Case, searchPrivilegeModel));
    assertTrue(collection1Case.implies(collection1, searchPrivilegeModel));

    assertFalse(collection1.implies(collection2, searchPrivilegeModel));
    assertFalse(collection1Case.implies(collection2, searchPrivilegeModel));
    assertFalse(collection2.implies(collection1, searchPrivilegeModel));
    assertFalse(collection2.implies(collection1Case, searchPrivilegeModel));
  }

  @Test
  public void testSimpleAction() throws Exception {
    CommonPrivilege query =
            create(new KeyValue("collection", "coll1"), new KeyValue("action", "query"));
    CommonPrivilege update =
            create(new KeyValue("collection", "coll1"), new KeyValue("action", "update"));
    CommonPrivilege queryCase =
            create(new KeyValue("colleCtIon", "coLl1"), new KeyValue("AcTiOn", "QuERy"));

    assertTrue(query.implies(query, searchPrivilegeModel));
    assertTrue(update.implies(update, searchPrivilegeModel));
    assertTrue(query.implies(queryCase, searchPrivilegeModel));
    assertTrue(queryCase.implies(query, searchPrivilegeModel));

    assertFalse(query.implies(update, searchPrivilegeModel));
    assertFalse(queryCase.implies(update, searchPrivilegeModel));
    assertFalse(update.implies(query, searchPrivilegeModel));
    assertFalse(update.implies(queryCase, searchPrivilegeModel));
  }

  @Test
  public void testRoleShorterThanRequest() throws Exception {
    CommonPrivilege collection1 = create(new KeyValue("collection", "coll1"));
    CommonPrivilege query =
            create(new KeyValue("collection", "coll1"), new KeyValue("action", "query"));
    CommonPrivilege update =
            create(new KeyValue("collection", "coll1"), new KeyValue("action", "update"));
    CommonPrivilege all =
            create(new KeyValue("collection", "coll1"), new KeyValue("action", ALL));

    assertTrue(collection1.implies(query, searchPrivilegeModel));
    assertTrue(collection1.implies(update, searchPrivilegeModel));
    assertTrue(collection1.implies(all, searchPrivilegeModel));

    assertFalse(query.implies(collection1, searchPrivilegeModel));
    assertFalse(update.implies(collection1, searchPrivilegeModel));
    assertTrue(all.implies(collection1, searchPrivilegeModel));
  }

  @Test
  public void testCollectionAll() throws Exception {
    CommonPrivilege collectionAll = create(new KeyValue("collection", ALL));
    CommonPrivilege collection1 = create(new KeyValue("collection", "coll1"));
    assertTrue(collectionAll.implies(collection1, searchPrivilegeModel));
    assertTrue(collection1.implies(collectionAll, searchPrivilegeModel));

    CommonPrivilege allUpdate =
            create(new KeyValue("collection", ALL), new KeyValue("action", "update"));
    CommonPrivilege allQuery =
            create(new KeyValue("collection", ALL), new KeyValue("action", "query"));
    CommonPrivilege coll1Update =
            create(new KeyValue("collection", "coll1"), new KeyValue("action", "update"));
    CommonPrivilege coll1Query =
            create(new KeyValue("collection", "coll1"), new KeyValue("action", "query"));
    assertTrue(allUpdate.implies(coll1Update, searchPrivilegeModel));
    assertTrue(allQuery.implies(coll1Query, searchPrivilegeModel));
    assertTrue(coll1Update.implies(allUpdate, searchPrivilegeModel));
    assertTrue(coll1Query.implies(allQuery, searchPrivilegeModel));
    assertFalse(allUpdate.implies(coll1Query, searchPrivilegeModel));
    assertFalse(coll1Update.implies(coll1Query, searchPrivilegeModel));
    assertFalse(allQuery.implies(coll1Update, searchPrivilegeModel));
    assertFalse(coll1Query.implies(allUpdate, searchPrivilegeModel));
    assertFalse(allUpdate.implies(allQuery, searchPrivilegeModel));
    assertFalse(allQuery.implies(allUpdate, searchPrivilegeModel));
    assertFalse(coll1Update.implies(coll1Query, searchPrivilegeModel));
    assertFalse(coll1Query.implies(coll1Update, searchPrivilegeModel));

    // test different length paths
    assertTrue(collectionAll.implies(allUpdate, searchPrivilegeModel));
    assertTrue(collectionAll.implies(allQuery, searchPrivilegeModel));
    assertTrue(collectionAll.implies(coll1Update, searchPrivilegeModel));
    assertTrue(collectionAll.implies(coll1Query, searchPrivilegeModel));
    assertFalse(allUpdate.implies(collectionAll, searchPrivilegeModel));
    assertFalse(allQuery.implies(collectionAll, searchPrivilegeModel));
    assertFalse(coll1Update.implies(collectionAll, searchPrivilegeModel));
    assertFalse(coll1Query.implies(collectionAll, searchPrivilegeModel));
  }

  @Test
  public void testActionAll() throws Exception {
    CommonPrivilege coll1All =
            create(new KeyValue("collection", "coll1"), new KeyValue("action", ALL));
    CommonPrivilege coll1Update =
            create(new KeyValue("collection", "coll1"), new KeyValue("action", "update"));
    CommonPrivilege coll1Query =
            create(new KeyValue("collection", "coll1"), new KeyValue("action", "query"));
    assertTrue(coll1All.implies(coll1All, searchPrivilegeModel));
    assertTrue(coll1All.implies(coll1Update, searchPrivilegeModel));
    assertTrue(coll1All.implies(coll1Query, searchPrivilegeModel));
    assertFalse(coll1Update.implies(coll1All, searchPrivilegeModel));
    assertFalse(coll1Query.implies(coll1All, searchPrivilegeModel));

    // test different lengths
    CommonPrivilege coll1 =
            create(new KeyValue("collection", "coll1"));
    assertTrue(coll1All.implies(coll1, searchPrivilegeModel));
    assertTrue(coll1.implies(coll1All, searchPrivilegeModel));
  }

  @Test
  public void testUnexpected() throws Exception {
    Privilege p = new Privilege() {
      @Override
      public boolean implies(Privilege p, Model m) {
        return false;
      }

      @Override
      public List<KeyValue> getAuthorizable() {
        return null;
      }
    };
    Privilege collection1 = create(new KeyValue("collection", "coll1"));
    assertFalse(collection1.implies(null, searchPrivilegeModel));
    assertFalse(collection1.implies(p, searchPrivilegeModel));
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
    System.out.println(create(SentryConstants.KV_JOINER.join("collection", "")));
  }

  @Test(expected=IllegalArgumentException.class)
  public void testEmptyValue() throws Exception {
    System.out.println(create(SentryConstants.KV_JOINER.join("", "coll1")));
  }

  @Test(expected=IllegalArgumentException.class)
  public void testEmptyPart() throws Exception {
    System.out.println(create(SentryConstants.AUTHORIZABLE_JOINER.
            join(SentryConstants.KV_JOINER.join("collection1", "coll1"), "")));
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
