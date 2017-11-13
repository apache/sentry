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
import org.apache.sentry.core.model.solr.SolrConstants;
import org.apache.sentry.core.model.solr.SolrPrivilegeModel;
import org.apache.sentry.policy.common.CommonPrivilege;
import org.apache.sentry.policy.common.Privilege;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;

/**
 * This class implements a set of unit tests designed to verify {@linkplain SolrPrivilegeModel}
 */
public class TestCommonPrivilegeForSolr {

  private Model solrPrivilegeModel;

  private static final String ALL = SolrConstants.ALL;

  @Before
  public void prepareData() {
    solrPrivilegeModel = SolrPrivilegeModel.getInstance();
  }

  @Test
  public void testSimpleNoAction() throws Exception {
    CommonPrivilege collection1 = create(new KeyValue("collection", "coll1"));
    CommonPrivilege collection2 = create(new KeyValue("collection", "coll2"));
    CommonPrivilege collection1Case = create(new KeyValue("colleCtIon", "coLl1"));

    assertTrue(collection1.implies(collection1, solrPrivilegeModel));
    assertTrue(collection2.implies(collection2, solrPrivilegeModel));
    assertTrue(collection1.implies(collection1Case, solrPrivilegeModel));
    assertTrue(collection1Case.implies(collection1, solrPrivilegeModel));

    assertFalse(collection1.implies(collection2, solrPrivilegeModel));
    assertFalse(collection1Case.implies(collection2, solrPrivilegeModel));
    assertFalse(collection2.implies(collection1, solrPrivilegeModel));
    assertFalse(collection2.implies(collection1Case, solrPrivilegeModel));
  }

  @Test
  public void testAdminNoAction() throws Exception {
    CommonPrivilege globalAdmin = create(new KeyValue("admin", SolrConstants.ALL));
    CommonPrivilege coreAdmin = create(new KeyValue("admin", "core"));
    CommonPrivilege collectionAdmin = create(new KeyValue("admin", "collection"));
    CommonPrivilege securityAdmin = create(new KeyValue("admin", "security"));

    assertTrue(coreAdmin.implies(coreAdmin, solrPrivilegeModel));
    assertFalse(coreAdmin.implies(collectionAdmin, solrPrivilegeModel));
    assertFalse(coreAdmin.implies(securityAdmin, solrPrivilegeModel));
    // TODO - Check if this is a bug ?
    // assertFalse(coreAdmin.implies(globalAdmin, solrPrivilegeModel));

    assertTrue(collectionAdmin.implies(collectionAdmin, solrPrivilegeModel));
    assertFalse(collectionAdmin.implies(coreAdmin, solrPrivilegeModel));
    assertFalse(collectionAdmin.implies(securityAdmin, solrPrivilegeModel));
    // TODO - Check if this is a bug ?
    // assertFalse(collectionAdmin.implies(globalAdmin, solrPrivilegeModel));

    assertTrue(securityAdmin.implies(securityAdmin, solrPrivilegeModel));
    assertFalse(securityAdmin.implies(collectionAdmin, solrPrivilegeModel));
    assertFalse(securityAdmin.implies(coreAdmin, solrPrivilegeModel));
    // TODO - Check if this is a bug ?
    // assertFalse(securityAdmin.implies(globalAdmin, solrPrivilegeModel));

    assertTrue(globalAdmin.implies(globalAdmin, solrPrivilegeModel));
    assertTrue(globalAdmin.implies(collectionAdmin, solrPrivilegeModel));
    assertTrue(globalAdmin.implies(coreAdmin, solrPrivilegeModel));
    assertTrue(globalAdmin.implies(securityAdmin, solrPrivilegeModel));
  }

  @Test
  public void testSimpleAction() throws Exception {
    CommonPrivilege query =
        create(new KeyValue("collection", "coll1"), new KeyValue("action", "query"));
    CommonPrivilege update =
        create(new KeyValue("collection", "coll1"), new KeyValue("action", "update"));
    CommonPrivilege queryCase =
        create(new KeyValue("colleCtIon", "coLl1"), new KeyValue("AcTiOn", "QuERy"));

    assertTrue(query.implies(query, solrPrivilegeModel));
    assertTrue(update.implies(update, solrPrivilegeModel));
    assertTrue(query.implies(queryCase, solrPrivilegeModel));
    assertTrue(queryCase.implies(query, solrPrivilegeModel));

    assertFalse(query.implies(update, solrPrivilegeModel));
    assertFalse(queryCase.implies(update, solrPrivilegeModel));
    assertFalse(update.implies(query, solrPrivilegeModel));
    assertFalse(update.implies(queryCase, solrPrivilegeModel));
  }

  @Test
  public void testAdminAction() throws Exception {
    CommonPrivilege query =
        create(new KeyValue("admin", SolrConstants.ALL), new KeyValue("action", "query"));
    CommonPrivilege update =
        create(new KeyValue("admin", SolrConstants.ALL), new KeyValue("action", "update"));
    CommonPrivilege queryCase =
        create(new KeyValue("admin", SolrConstants.ALL), new KeyValue("AcTiOn", "QuERy"));

    assertTrue(query.implies(query, solrPrivilegeModel));
    assertTrue(update.implies(update, solrPrivilegeModel));
    assertTrue(query.implies(queryCase, solrPrivilegeModel));
    assertTrue(queryCase.implies(query, solrPrivilegeModel));

    assertFalse(query.implies(update, solrPrivilegeModel));
    assertFalse(queryCase.implies(update, solrPrivilegeModel));
    assertFalse(update.implies(query, solrPrivilegeModel));
    assertFalse(update.implies(queryCase, solrPrivilegeModel));
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

    assertTrue(collection1.implies(query, solrPrivilegeModel));
    assertTrue(collection1.implies(update, solrPrivilegeModel));
    assertTrue(collection1.implies(all, solrPrivilegeModel));

    assertFalse(query.implies(collection1, solrPrivilegeModel));
    assertFalse(update.implies(collection1, solrPrivilegeModel));
    assertTrue(all.implies(collection1, solrPrivilegeModel));
  }

  @Test
  public void testAdminRoleShorterThanRequest() throws Exception {
    CommonPrivilege globalAdmin = create(new KeyValue("admin", "*"));
    CommonPrivilege query =
        create(new KeyValue("admin", "core"), new KeyValue("action", "query"));
    CommonPrivilege update =
        create(new KeyValue("admin", "core"), new KeyValue("action", "update"));
    CommonPrivilege all = create(new KeyValue("admin", "*"), new KeyValue("action", ALL));

    assertTrue(globalAdmin.implies(query, solrPrivilegeModel));
    assertTrue(globalAdmin.implies(update, solrPrivilegeModel));
    assertTrue(globalAdmin.implies(all, solrPrivilegeModel));

    assertFalse(query.implies(globalAdmin, solrPrivilegeModel));
    assertFalse(update.implies(globalAdmin, solrPrivilegeModel));
    assertTrue(all.implies(globalAdmin, solrPrivilegeModel));
  }

  @Test
  public void testCollectionAll() throws Exception {
    CommonPrivilege collectionAll = create(new KeyValue("collection", ALL));
    CommonPrivilege collection1 = create(new KeyValue("collection", "coll1"));
    assertTrue(collectionAll.implies(collection1, solrPrivilegeModel));
    assertTrue(collection1.implies(collectionAll, solrPrivilegeModel));

    CommonPrivilege allUpdate =
        create(new KeyValue("collection", ALL), new KeyValue("action", "update"));
    CommonPrivilege allQuery =
        create(new KeyValue("collection", ALL), new KeyValue("action", "query"));
    CommonPrivilege coll1Update =
        create(new KeyValue("collection", "coll1"), new KeyValue("action", "update"));
    CommonPrivilege coll1Query =
        create(new KeyValue("collection", "coll1"), new KeyValue("action", "query"));
    assertTrue(allUpdate.implies(coll1Update, solrPrivilegeModel));
    assertTrue(allQuery.implies(coll1Query, solrPrivilegeModel));
    assertTrue(coll1Update.implies(allUpdate, solrPrivilegeModel));
    assertTrue(coll1Query.implies(allQuery, solrPrivilegeModel));
    assertFalse(allUpdate.implies(coll1Query, solrPrivilegeModel));
    assertFalse(coll1Update.implies(coll1Query, solrPrivilegeModel));
    assertFalse(allQuery.implies(coll1Update, solrPrivilegeModel));
    assertFalse(coll1Query.implies(allUpdate, solrPrivilegeModel));
    assertFalse(allUpdate.implies(allQuery, solrPrivilegeModel));
    assertFalse(allQuery.implies(allUpdate, solrPrivilegeModel));
    assertFalse(coll1Update.implies(coll1Query, solrPrivilegeModel));
    assertFalse(coll1Query.implies(coll1Update, solrPrivilegeModel));

    // test different length paths
    assertTrue(collectionAll.implies(allUpdate, solrPrivilegeModel));
    assertTrue(collectionAll.implies(allQuery, solrPrivilegeModel));
    assertTrue(collectionAll.implies(coll1Update, solrPrivilegeModel));
    assertTrue(collectionAll.implies(coll1Query, solrPrivilegeModel));
    assertFalse(allUpdate.implies(collectionAll, solrPrivilegeModel));
    assertFalse(allQuery.implies(collectionAll, solrPrivilegeModel));
    assertFalse(coll1Update.implies(collectionAll, solrPrivilegeModel));
    assertFalse(coll1Query.implies(collectionAll, solrPrivilegeModel));
  }

  @Test
  public void testActionAll() throws Exception {
    CommonPrivilege coll1All =
        create(new KeyValue("collection", "coll1"), new KeyValue("action", ALL));
    CommonPrivilege coll1Update =
        create(new KeyValue("collection", "coll1"), new KeyValue("action", "update"));
    CommonPrivilege coll1Query =
        create(new KeyValue("collection", "coll1"), new KeyValue("action", "query"));
    assertTrue(coll1All.implies(coll1All, solrPrivilegeModel));
    assertTrue(coll1All.implies(coll1Update, solrPrivilegeModel));
    assertTrue(coll1All.implies(coll1Query, solrPrivilegeModel));
    assertFalse(coll1Update.implies(coll1All, solrPrivilegeModel));
    assertFalse(coll1Query.implies(coll1All, solrPrivilegeModel));

    // test different lengths
    CommonPrivilege coll1 =
        create(new KeyValue("collection", "coll1"));
    assertTrue(coll1All.implies(coll1, solrPrivilegeModel));
    assertTrue(coll1.implies(coll1All, solrPrivilegeModel));
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
    assertFalse(collection1.implies(null, solrPrivilegeModel));
    assertFalse(collection1.implies(p, solrPrivilegeModel));
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
