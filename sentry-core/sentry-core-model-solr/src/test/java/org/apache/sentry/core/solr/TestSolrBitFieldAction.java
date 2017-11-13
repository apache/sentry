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
package org.apache.sentry.core.solr;

import org.apache.sentry.core.model.solr.SolrActionFactory;
import org.apache.sentry.core.model.solr.SolrConstants;
import org.apache.sentry.core.model.solr.SolrActionFactory.SolrAction;
import org.apache.sentry.core.model.solr.SolrActionFactory.SolrBitFieldAction;
import org.junit.Test;

import com.google.common.collect.Lists;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertEquals;

public class TestSolrBitFieldAction {
  SolrActionFactory actionFactory = new SolrActionFactory();

  @Test
  public void testImpliesAction() {
    SolrBitFieldAction updateAction = new SolrBitFieldAction(SolrAction.UPDATE);
    SolrBitFieldAction queryAction = new SolrBitFieldAction(SolrAction.QUERY);
    SolrBitFieldAction allAction = new SolrBitFieldAction(SolrAction.ALL);

    assertTrue(allAction.implies(queryAction));
    assertTrue(allAction.implies(updateAction));
    assertTrue(allAction.implies(allAction));
    assertTrue(updateAction.implies(updateAction));
    assertTrue(queryAction.implies(queryAction));

    assertFalse(queryAction.implies(updateAction));
    assertFalse(queryAction.implies(allAction));
    assertFalse(updateAction.implies(queryAction));
    assertFalse(updateAction.implies(allAction));
  }

  @Test
  public void testGetActionByName() throws Exception {
    SolrBitFieldAction updateAction = (SolrBitFieldAction)actionFactory.getActionByName(SolrConstants.UPDATE);
    SolrBitFieldAction queryAction = (SolrBitFieldAction)actionFactory.getActionByName(SolrConstants.QUERY);
    SolrBitFieldAction allAction = (SolrBitFieldAction)actionFactory.getActionByName(SolrConstants.ALL);

    assertTrue(updateAction.equals(new SolrBitFieldAction(SolrAction.UPDATE)));
    assertTrue(queryAction.equals(new SolrBitFieldAction(SolrAction.QUERY)));
    assertTrue(allAction.equals(new SolrBitFieldAction(SolrAction.ALL)));
  }

  @Test
  public void testGetActionsByCode() throws Exception {
    SolrBitFieldAction updateAction = new SolrBitFieldAction(SolrAction.UPDATE);
    SolrBitFieldAction queryAction = new SolrBitFieldAction(SolrAction.QUERY);

    assertEquals(Lists.newArrayList(updateAction, queryAction), actionFactory.getActionsByCode(SolrAction.ALL.getCode()));
    assertEquals(Lists.newArrayList(updateAction), actionFactory.getActionsByCode(SolrAction.UPDATE.getCode()));
    assertEquals(Lists.newArrayList(queryAction), actionFactory.getActionsByCode(SolrAction.QUERY.getCode()));
  }
}
