/**
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sentry.provider.db.generic.service.persistent;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import java.util.Arrays;

import org.apache.sentry.core.model.db.AccessConstants;
import org.apache.sentry.core.model.solr.Collection;
import org.apache.sentry.core.model.solr.Field;
import org.apache.sentry.core.model.solr.SolrConstants;
import org.apache.sentry.provider.db.service.model.MSentryGMPrivilege;
import org.junit.Test;

public class TestSentryGMPrivilege {

  @Test
  public void testValidateAuthorizables() throws Exception {
    try {
      new MSentryGMPrivilege("solr",
          "service1", Arrays.asList(new Collection("c1"), new Field("f1")),SolrConstants.QUERY, false);
    } catch (IllegalStateException e) {
      fail("unexpect happend: it is a validated privilege");
    }

    try {
      new MSentryGMPrivilege("solr",
          "service1", Arrays.asList(new Collection(""), new Field("f1")),SolrConstants.QUERY, false);
      fail("unexpect happend: it is not a validated privilege, The empty name of authorizable can't be empty");
    } catch (IllegalStateException e) {
    }

    try {
      new MSentryGMPrivilege("solr",
          "service1", Arrays.asList(null, new Field("f1")),SolrConstants.QUERY, false);
      fail("unexpect happend: it is not a validated privilege, The authorizable can't be null");
    } catch (IllegalStateException e) {
    }
  }

  @Test
  public void testImpliesWithServerScope() throws Exception {
    //The persistent privilege is server scope
    MSentryGMPrivilege serverPrivilege = new MSentryGMPrivilege("solr",
        "service1", null,SolrConstants.QUERY, false);

    MSentryGMPrivilege collectionPrivilege = new MSentryGMPrivilege("solr",
        "service1", Arrays.asList(new Collection("c1")),
        SolrConstants.QUERY, false);
    assertTrue(serverPrivilege.implies(collectionPrivilege));

    MSentryGMPrivilege fieldPrivilege = new MSentryGMPrivilege("solr",
        "service1", Arrays.asList(new Collection("c1"), new Field("f1")),
        SolrConstants.QUERY, false);
    assertTrue(serverPrivilege.implies(fieldPrivilege));
    assertTrue(collectionPrivilege.implies(fieldPrivilege));

    serverPrivilege.setAction(SolrConstants.UPDATE);
    assertFalse(serverPrivilege.implies(collectionPrivilege));
    assertFalse(serverPrivilege.implies(fieldPrivilege));

    serverPrivilege.setAction(SolrConstants.ALL);
    assertTrue(serverPrivilege.implies(collectionPrivilege));
    assertTrue(serverPrivilege.implies(fieldPrivilege));
  }
  /**
   * The requested privilege has the different authorizable size with the persistent privilege
   * @throws Exception
   */
  @Test
  public void testImpliesDifferentAuthorizable() throws Exception {
    /**
     * Test the scope of persistent privilege is the larger than the requested privilege
     */
    MSentryGMPrivilege serverPrivilege = new MSentryGMPrivilege("solr",
        "service1", null, SolrConstants.QUERY, false);

    MSentryGMPrivilege collectionPrivilege = new MSentryGMPrivilege("solr",
        "service1", Arrays.asList(new Collection("c1")),
        SolrConstants.QUERY, false);

    MSentryGMPrivilege fieldPrivilege = new MSentryGMPrivilege("solr",
        "service1", Arrays.asList(new Collection("c1"), new Field("f1")),
        SolrConstants.QUERY, false);
    assertTrue(serverPrivilege.implies(collectionPrivilege));
    assertTrue(serverPrivilege.implies(fieldPrivilege));
    assertTrue(collectionPrivilege.implies(fieldPrivilege));
    /**
     * Test the scope of persistent privilege is less than  the request privilege
     */
    assertFalse(fieldPrivilege.implies(collectionPrivilege));
    assertFalse(fieldPrivilege.implies(serverPrivilege));
    assertFalse(collectionPrivilege.implies(serverPrivilege));

    /**
     * Test the scope of persistent privilege is less than  the request privilege,
     * but the name of left authorizable is ALL
     */
    MSentryGMPrivilege fieldAllPrivilege = new MSentryGMPrivilege("solr",
        "service1", Arrays.asList(new Collection("c1"), new Field(AccessConstants.ALL)),
        SolrConstants.QUERY, false);

    assertTrue(fieldAllPrivilege.implies(collectionPrivilege));

    /**
     * Test the scope of persistent privilege has the same scope as request privilege
     */
    MSentryGMPrivilege fieldPrivilege1 = new MSentryGMPrivilege("solr",
        "service1", Arrays.asList(new Collection("c1"), new Field("f1")),
        SolrConstants.QUERY, false);

    MSentryGMPrivilege fieldPrivilege2 = new MSentryGMPrivilege("solr",
        "service1", Arrays.asList(new Collection("c2"), new Field("f2")),
        SolrConstants.QUERY, false);
    assertFalse(fieldPrivilege1.implies(fieldPrivilege2));
  }

  /**
   * The requested privilege has the same authorizable size as with the persistent privilege
   * @throws Exception
   */
  @Test
  public void testSearchImpliesEqualAuthorizable() throws Exception {

    MSentryGMPrivilege serverPrivilege1 = new MSentryGMPrivilege("solr",
        "service1", null,SolrConstants.QUERY, false);

    MSentryGMPrivilege serverPrivilege2 = new MSentryGMPrivilege("solr",
        "service2", null,SolrConstants.QUERY, false);

    assertFalse(serverPrivilege1.implies(serverPrivilege2));

    MSentryGMPrivilege collectionPrivilege1 = new MSentryGMPrivilege("solr",
        "service1", Arrays.asList(new Collection("c1")),
        SolrConstants.QUERY, false);

    MSentryGMPrivilege collectionPrivilege2 = new MSentryGMPrivilege("solr",
        "service1", Arrays.asList(new Collection("c2")),
        SolrConstants.QUERY, false);

    assertFalse(collectionPrivilege1.implies(collectionPrivilege2));

    MSentryGMPrivilege fieldPrivilege1 = new MSentryGMPrivilege("solr",
        "service1", Arrays.asList(new Collection("c1"), new Field("f1")),
        SolrConstants.QUERY, false);

    MSentryGMPrivilege fieldPrivilege2 = new MSentryGMPrivilege("solr",
        "service1", Arrays.asList(new Collection("c1"), new Field("f2")),
        SolrConstants.QUERY, false);

    assertFalse(fieldPrivilege1.implies(fieldPrivilege2));

    /**
     * The authorizables aren't equal,but the persistent privilege has the ALL name
     */
    collectionPrivilege2.setAuthorizables(Arrays.asList(new Collection(AccessConstants.ALL)));
    collectionPrivilege2.implies(collectionPrivilege1);

    fieldPrivilege2.setAuthorizables(Arrays.asList(new Collection("c1"), new Field(AccessConstants.ALL)));
    fieldPrivilege2.implies(fieldPrivilege1);
  }

  @Test
  public void testSearchImpliesAction() throws Exception {
    /**
     * action is equal
     */
    MSentryGMPrivilege fieldPrivilege1 = new MSentryGMPrivilege("solr",
        "service1", Arrays.asList(new Collection("c1"), new Field("f2")),
        SolrConstants.QUERY, false);

    MSentryGMPrivilege fieldPrivilege2 = new MSentryGMPrivilege("solr",
        "service1", Arrays.asList(new Collection("c1"), new Field("f2")),
        SolrConstants.QUERY, false);

    assertTrue(fieldPrivilege1.implies(fieldPrivilege2));

    /**
     * action isn't equal
     */
    fieldPrivilege2.setAction(SolrConstants.UPDATE);
    assertFalse(fieldPrivilege1.implies(fieldPrivilege2));
    /**
     * action isn't equal,but the persistent privilege has the ALL action
     */
    fieldPrivilege1.setAction(SolrConstants.ALL);
    assertTrue(fieldPrivilege1.implies(fieldPrivilege2));
  }
}
