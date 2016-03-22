/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.sentry.core.model.kafka;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;

import org.apache.sentry.core.model.kafka.KafkaActionFactory.KafkaAction;
import org.junit.Test;

import com.google.common.collect.Lists;

/**
 * Test KafkaActionFactory creates expected Kafka action instances.
 */
public class TestKafkaAction {
  private KafkaActionFactory factory = KafkaActionFactory.getInstance();

  @Test
  public void testImpliesAction() {
    KafkaAction readAction = (KafkaAction) factory.getActionByName(KafkaActionConstant.READ);
    KafkaAction writeAction = (KafkaAction) factory.getActionByName(KafkaActionConstant.WRITE);
    KafkaAction createAction = (KafkaAction) factory.getActionByName(KafkaActionConstant.CREATE);
    KafkaAction deleteAction = (KafkaAction) factory.getActionByName(KafkaActionConstant.DELETE);
    KafkaAction alterAction = (KafkaAction) factory.getActionByName(KafkaActionConstant.ALTER);
    KafkaAction describeAction =
        (KafkaAction) factory.getActionByName(KafkaActionConstant.DESCRIBE);
    KafkaAction adminAction = (KafkaAction) factory.getActionByName(KafkaActionConstant.CLUSTER_ACTION);
    KafkaAction allAction = (KafkaAction) factory.getActionByName(KafkaActionConstant.ALL);

    assertTrue(allAction.implies(readAction));
    assertTrue(allAction.implies(writeAction));
    assertTrue(allAction.implies(createAction));
    assertTrue(allAction.implies(deleteAction));
    assertTrue(allAction.implies(alterAction));
    assertTrue(allAction.implies(describeAction));
    assertTrue(allAction.implies(adminAction));
    assertTrue(allAction.implies(allAction));

    assertTrue(readAction.implies(readAction));
    assertFalse(readAction.implies(writeAction));
    assertFalse(readAction.implies(createAction));
    assertFalse(readAction.implies(deleteAction));
    assertFalse(readAction.implies(alterAction));
    assertFalse(readAction.implies(describeAction));
    assertFalse(readAction.implies(adminAction));
    assertFalse(readAction.implies(allAction));

    assertFalse(writeAction.implies(readAction));
    assertTrue(writeAction.implies(writeAction));
    assertFalse(writeAction.implies(createAction));
    assertFalse(writeAction.implies(deleteAction));
    assertFalse(writeAction.implies(alterAction));
    assertFalse(writeAction.implies(describeAction));
    assertFalse(writeAction.implies(adminAction));
    assertFalse(writeAction.implies(allAction));

    assertFalse(createAction.implies(readAction));
    assertFalse(createAction.implies(writeAction));
    assertTrue(createAction.implies(createAction));
    assertFalse(createAction.implies(deleteAction));
    assertFalse(createAction.implies(alterAction));
    assertFalse(createAction.implies(describeAction));
    assertFalse(createAction.implies(adminAction));
    assertFalse(createAction.implies(allAction));

    assertFalse(deleteAction.implies(readAction));
    assertFalse(deleteAction.implies(writeAction));
    assertFalse(deleteAction.implies(createAction));
    assertTrue(deleteAction.implies(deleteAction));
    assertFalse(deleteAction.implies(alterAction));
    assertFalse(deleteAction.implies(describeAction));
    assertFalse(deleteAction.implies(adminAction));
    assertFalse(deleteAction.implies(allAction));

    assertFalse(alterAction.implies(readAction));
    assertFalse(alterAction.implies(writeAction));
    assertFalse(alterAction.implies(createAction));
    assertFalse(alterAction.implies(deleteAction));
    assertTrue(alterAction.implies(alterAction));
    assertFalse(alterAction.implies(describeAction));
    assertFalse(alterAction.implies(adminAction));
    assertFalse(alterAction.implies(allAction));

    assertFalse(describeAction.implies(readAction));
    assertFalse(describeAction.implies(writeAction));
    assertFalse(describeAction.implies(createAction));
    assertFalse(describeAction.implies(deleteAction));
    assertFalse(describeAction.implies(alterAction));
    assertTrue(describeAction.implies(describeAction));
    assertFalse(describeAction.implies(adminAction));
    assertFalse(describeAction.implies(allAction));

    assertFalse(adminAction.implies(readAction));
    assertFalse(adminAction.implies(writeAction));
    assertFalse(adminAction.implies(createAction));
    assertFalse(adminAction.implies(deleteAction));
    assertFalse(adminAction.implies(alterAction));
    assertFalse(adminAction.implies(describeAction));
    assertTrue(adminAction.implies(adminAction));
    assertFalse(adminAction.implies(allAction));
  }

  @Test
  public void testGetActionByName() throws Exception {
    KafkaAction readAction = (KafkaAction) factory.getActionByName(KafkaActionConstant.READ);
    KafkaAction writeAction = (KafkaAction) factory.getActionByName(KafkaActionConstant.WRITE);
    KafkaAction createAction = (KafkaAction) factory.getActionByName(KafkaActionConstant.CREATE);
    KafkaAction deleteAction = (KafkaAction) factory.getActionByName(KafkaActionConstant.DELETE);
    KafkaAction alterAction = (KafkaAction) factory.getActionByName(KafkaActionConstant.ALTER);
    KafkaAction describeAction =
        (KafkaAction) factory.getActionByName(KafkaActionConstant.DESCRIBE);
    KafkaAction adminAction = (KafkaAction) factory.getActionByName(KafkaActionConstant.CLUSTER_ACTION);
    KafkaAction allAction = (KafkaAction) factory.getActionByName(KafkaActionConstant.ALL);

    assertTrue(readAction.equals(new KafkaAction(KafkaActionConstant.READ)));
    assertTrue(writeAction.equals(new KafkaAction(KafkaActionConstant.WRITE)));
    assertTrue(createAction.equals(new KafkaAction(KafkaActionConstant.CREATE)));
    assertTrue(deleteAction.equals(new KafkaAction(KafkaActionConstant.DELETE)));
    assertTrue(alterAction.equals(new KafkaAction(KafkaActionConstant.ALTER)));
    assertTrue(describeAction.equals(new KafkaAction(KafkaActionConstant.DESCRIBE)));
    assertTrue(adminAction.equals(new KafkaAction(KafkaActionConstant.CLUSTER_ACTION)));
    assertTrue(allAction.equals(new KafkaAction(KafkaActionConstant.ALL)));
  }

  @Test
  public void testGetActionsByCode() throws Exception {
    KafkaAction readAction = new KafkaAction(KafkaActionConstant.READ);
    KafkaAction writeAction = new KafkaAction(KafkaActionConstant.WRITE);
    KafkaAction createAction = (KafkaAction) factory.getActionByName(KafkaActionConstant.CREATE);
    KafkaAction deleteAction = (KafkaAction) factory.getActionByName(KafkaActionConstant.DELETE);
    KafkaAction alterAction = (KafkaAction) factory.getActionByName(KafkaActionConstant.ALTER);
    KafkaAction describeAction =
        (KafkaAction) factory.getActionByName(KafkaActionConstant.DESCRIBE);
    KafkaAction adminAction = (KafkaAction) factory.getActionByName(KafkaActionConstant.CLUSTER_ACTION);
    KafkaAction allAction = new KafkaAction(KafkaActionConstant.ALL);

    assertEquals(Lists.newArrayList(readAction),
        factory.getActionsByCode(readAction.getActionCode()));
    assertEquals(Lists.newArrayList(writeAction),
        factory.getActionsByCode(writeAction.getActionCode()));
    assertEquals(Lists.newArrayList(createAction),
        factory.getActionsByCode(createAction.getActionCode()));
    assertEquals(Lists.newArrayList(deleteAction),
        factory.getActionsByCode(deleteAction.getActionCode()));
    assertEquals(Lists.newArrayList(alterAction),
        factory.getActionsByCode(alterAction.getActionCode()));
    assertEquals(Lists.newArrayList(describeAction),
        factory.getActionsByCode(describeAction.getActionCode()));
    assertEquals(Lists.newArrayList(adminAction),
        factory.getActionsByCode(adminAction.getActionCode()));
    assertEquals(Lists.newArrayList(readAction, writeAction, createAction, deleteAction,
        alterAction, describeAction, adminAction), factory.getActionsByCode(allAction
        .getActionCode()));
  }

  @Test
  public void testGetActionForInvalidName() {
    assertEquals("Failed to NOT create Kafka action for invalid name.", null, factory.getActionByName("INVALID"));
  }

  @Test
  public void testGetActionForInvalidCode() {
    assertEquals("Failed to NOT create Kafka actions for invalid code.", 0, factory.getActionsByCode(0).size());
  }
}
