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
package org.apache.sentry.core.search;

import java.util.List;

import org.apache.sentry.core.model.search.SearchActionFactory;
import org.apache.sentry.core.model.search.SearchActionFactory.SearchAction;
import org.apache.sentry.core.model.search.SearchActionFactory.SearchBitFieldAction;
import org.apache.sentry.core.model.search.SearchConstants;
import org.junit.Test;

import com.google.common.collect.Lists;

import static junit.framework.Assert.assertTrue;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertEquals;

public class TestSearchBitFieldAction {
  SearchActionFactory actionFactory = new SearchActionFactory();

  @Test
  public void testImpliesAction() {
    SearchBitFieldAction updateAction = new SearchBitFieldAction(SearchAction.UPDATE);
    SearchBitFieldAction queryAction = new SearchBitFieldAction(SearchAction.QUERY);
    SearchBitFieldAction allAction = new SearchBitFieldAction(SearchAction.ALL);

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
    SearchBitFieldAction updateAction = (SearchBitFieldAction)actionFactory.getActionByName(SearchConstants.UPDATE);
    SearchBitFieldAction queryAction = (SearchBitFieldAction)actionFactory.getActionByName(SearchConstants.QUERY);
    SearchBitFieldAction allAction = (SearchBitFieldAction)actionFactory.getActionByName(SearchConstants.ALL);

    assertTrue(updateAction.equals(new SearchBitFieldAction(SearchAction.UPDATE)));
    assertTrue(queryAction.equals(new SearchBitFieldAction(SearchAction.QUERY)));
    assertTrue(allAction.equals(new SearchBitFieldAction(SearchAction.ALL)));
  }

  @Test
  public void testGetActionsByCode() throws Exception {
    SearchBitFieldAction updateAction = new SearchBitFieldAction(SearchAction.UPDATE);
    SearchBitFieldAction queryAction = new SearchBitFieldAction(SearchAction.QUERY);

    assertEquals(Lists.newArrayList(updateAction, queryAction), actionFactory.getActionsByCode(SearchAction.ALL.getCode()));
    assertEquals(Lists.newArrayList(updateAction), actionFactory.getActionsByCode(SearchAction.UPDATE.getCode()));
    assertEquals(Lists.newArrayList(queryAction), actionFactory.getActionsByCode(SearchAction.QUERY.getCode()));
  }
}
