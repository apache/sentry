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
package org.apache.sentry.core.indexer;

import java.util.List;

import org.apache.sentry.core.model.indexer.IndexerActionFactory;
import org.apache.sentry.core.model.indexer.IndexerActionFactory.IndexerAction;
import org.apache.sentry.core.model.indexer.IndexerActionFactory.IndexerBitFieldAction;
import org.apache.sentry.core.model.indexer.IndexerConstants;
import org.junit.Test;

import com.google.common.collect.Lists;

import static junit.framework.Assert.assertTrue;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertEquals;

public class TestIndexerBitFieldAction {
  IndexerActionFactory actionFactory = new IndexerActionFactory();

  @Test
  public void testImpliesAction() {
    IndexerBitFieldAction writeAction = new IndexerBitFieldAction(IndexerAction.WRITE);
    IndexerBitFieldAction readAction = new IndexerBitFieldAction(IndexerAction.READ);
    IndexerBitFieldAction allAction = new IndexerBitFieldAction(IndexerAction.ALL);

    assertTrue(allAction.implies(readAction));
    assertTrue(allAction.implies(writeAction));
    assertTrue(allAction.implies(allAction));
    assertTrue(writeAction.implies(writeAction));
    assertTrue(readAction.implies(readAction));

    assertFalse(readAction.implies(writeAction));
    assertFalse(readAction.implies(allAction));
    assertFalse(writeAction.implies(readAction));
    assertFalse(writeAction.implies(allAction));
  }

  @Test
  public void testGetActionByName() throws Exception {
    IndexerBitFieldAction writeAction = (IndexerBitFieldAction)actionFactory.getActionByName(IndexerConstants.WRITE);
    IndexerBitFieldAction readAction = (IndexerBitFieldAction)actionFactory.getActionByName(IndexerConstants.READ);
    IndexerBitFieldAction allAction = (IndexerBitFieldAction)actionFactory.getActionByName(IndexerConstants.ALL);

    assertTrue(writeAction.equals(new IndexerBitFieldAction(IndexerAction.WRITE)));
    assertTrue(readAction.equals(new IndexerBitFieldAction(IndexerAction.READ)));
    assertTrue(allAction.equals(new IndexerBitFieldAction(IndexerAction.ALL)));
  }

  @Test
  public void testGetActionsByCode() throws Exception {
    IndexerBitFieldAction writeAction = new IndexerBitFieldAction(IndexerAction.WRITE);
    IndexerBitFieldAction readAction = new IndexerBitFieldAction(IndexerAction.READ);

    assertEquals(Lists.newArrayList(writeAction, readAction), actionFactory.getActionsByCode(IndexerAction.ALL.getCode()));
    assertEquals(Lists.newArrayList(writeAction), actionFactory.getActionsByCode(IndexerAction.WRITE.getCode()));
    assertEquals(Lists.newArrayList(readAction), actionFactory.getActionsByCode(IndexerAction.READ.getCode()));
  }
}
