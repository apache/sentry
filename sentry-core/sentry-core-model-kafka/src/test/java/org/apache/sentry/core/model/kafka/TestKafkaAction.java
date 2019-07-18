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
  public void testAllActionImpliesAll() {
    KafkaAction allAction = factory.getActionByName(KafkaActionConstant.ALL);

    assertTrue(allAction.implies(factory.getActionByName(KafkaActionConstant.READ)));
    assertTrue(allAction.implies(factory.getActionByName(KafkaActionConstant.WRITE)));
    assertTrue(allAction.implies(factory.getActionByName(KafkaActionConstant.CREATE)));
    assertTrue(allAction.implies(factory.getActionByName(KafkaActionConstant.DELETE)));
    assertTrue(allAction.implies(factory.getActionByName(KafkaActionConstant.ALTER)));
    assertTrue(allAction.implies(factory.getActionByName(KafkaActionConstant.DESCRIBE)));
    assertTrue(allAction.implies(factory.getActionByName(KafkaActionConstant.CLUSTER_ACTION)));
    assertTrue(allAction.implies(factory.getActionByName(KafkaActionConstant.ALTER_CONFIGS)));
    assertTrue(allAction.implies(factory.getActionByName(KafkaActionConstant.DESCRIBE_CONFIGS)));
    assertTrue(allAction.implies(factory.getActionByName(KafkaActionConstant.IDEMPOTENT_WRITE)));
    assertTrue(allAction.implies(factory.getActionByName(KafkaActionConstant.ALL)));
  }

  @Test
  public void testActionImpliesSelf() {
    KafkaAction[] actions = new KafkaAction[]{
      factory.getActionByName(KafkaActionConstant.READ),
      factory.getActionByName(KafkaActionConstant.WRITE),
      factory.getActionByName(KafkaActionConstant.CREATE),
      factory.getActionByName(KafkaActionConstant.DELETE),
      factory.getActionByName(KafkaActionConstant.ALTER),
      factory.getActionByName(KafkaActionConstant.DESCRIBE),
      factory.getActionByName(KafkaActionConstant.CLUSTER_ACTION),
      factory.getActionByName(KafkaActionConstant.ALTER_CONFIGS),
      factory.getActionByName(KafkaActionConstant.DESCRIBE_CONFIGS),
      factory.getActionByName(KafkaActionConstant.IDEMPOTENT_WRITE),
      factory.getActionByName(KafkaActionConstant.ALL)
    };

    for(KafkaAction action : actions){
      assertTrue(action.implies(action));
    }
  }

  @Test
  public void testNonAllActionDoesNotImplyOthers() {
    KafkaAction allAction =  factory.getActionByName(KafkaActionConstant.ALL);

    KafkaAction[] actions = new KafkaAction[]{
      factory.getActionByName(KafkaActionConstant.READ),
      factory.getActionByName(KafkaActionConstant.WRITE),
      factory.getActionByName(KafkaActionConstant.CREATE),
      factory.getActionByName(KafkaActionConstant.DELETE),
      factory.getActionByName(KafkaActionConstant.ALTER),
      factory.getActionByName(KafkaActionConstant.DESCRIBE),
      factory.getActionByName(KafkaActionConstant.CLUSTER_ACTION),
      factory.getActionByName(KafkaActionConstant.ALTER_CONFIGS),
      factory.getActionByName(KafkaActionConstant.DESCRIBE_CONFIGS),
      factory.getActionByName(KafkaActionConstant.IDEMPOTENT_WRITE)
    };

    for(KafkaAction action : actions) {
      for(KafkaAction action2 : actions) {
        if (action != action2) {
          assertFalse(action.implies(action2));
        }
      }

      assertFalse(action.implies(allAction));
    }
  }

  @Test
  public void testGetActionByName() throws Exception {
    KafkaAction readAction = factory.getActionByName(KafkaActionConstant.READ);
    KafkaAction writeAction = factory.getActionByName(KafkaActionConstant.WRITE);
    KafkaAction createAction = factory.getActionByName(KafkaActionConstant.CREATE);
    KafkaAction deleteAction = factory.getActionByName(KafkaActionConstant.DELETE);
    KafkaAction alterAction = factory.getActionByName(KafkaActionConstant.ALTER);
    KafkaAction describeAction = factory.getActionByName(KafkaActionConstant.DESCRIBE);
    KafkaAction adminAction = factory.getActionByName(KafkaActionConstant.CLUSTER_ACTION);
    KafkaAction alterConfigsAction = factory.getActionByName(KafkaActionConstant.ALTER_CONFIGS);
    KafkaAction describeConfigsAction = factory.getActionByName(KafkaActionConstant.DESCRIBE_CONFIGS);
    KafkaAction idempotentWriteAction = factory.getActionByName(KafkaActionConstant.IDEMPOTENT_WRITE);
    KafkaAction allAction = factory.getActionByName(KafkaActionConstant.ALL);

    assertTrue(readAction.equals(new KafkaAction(KafkaActionConstant.READ)));
    assertTrue(writeAction.equals(new KafkaAction(KafkaActionConstant.WRITE)));
    assertTrue(createAction.equals(new KafkaAction(KafkaActionConstant.CREATE)));
    assertTrue(deleteAction.equals(new KafkaAction(KafkaActionConstant.DELETE)));
    assertTrue(alterAction.equals(new KafkaAction(KafkaActionConstant.ALTER)));
    assertTrue(describeAction.equals(new KafkaAction(KafkaActionConstant.DESCRIBE)));
    assertTrue(adminAction.equals(new KafkaAction(KafkaActionConstant.CLUSTER_ACTION)));
    assertTrue(alterConfigsAction.equals(new KafkaAction(KafkaActionConstant.ALTER_CONFIGS)));
    assertTrue(describeConfigsAction.equals(new KafkaAction(KafkaActionConstant.DESCRIBE_CONFIGS)));
    assertTrue(idempotentWriteAction.equals(new KafkaAction(KafkaActionConstant.IDEMPOTENT_WRITE)));
    assertTrue(allAction.equals(new KafkaAction(KafkaActionConstant.ALL)));
  }

  @Test
  public void testGetActionsByCode() throws Exception {
    KafkaAction readAction = factory.getActionByName(KafkaActionConstant.READ);
    KafkaAction writeAction = factory.getActionByName(KafkaActionConstant.WRITE);
    KafkaAction createAction = factory.getActionByName(KafkaActionConstant.CREATE);
    KafkaAction deleteAction = factory.getActionByName(KafkaActionConstant.DELETE);
    KafkaAction alterAction = factory.getActionByName(KafkaActionConstant.ALTER);
    KafkaAction describeAction = factory.getActionByName(KafkaActionConstant.DESCRIBE);
    KafkaAction adminAction = factory.getActionByName(KafkaActionConstant.CLUSTER_ACTION);
    KafkaAction alterConfigsAction = factory.getActionByName(KafkaActionConstant.ALTER_CONFIGS);
    KafkaAction describeConfigsAction = factory.getActionByName(KafkaActionConstant.DESCRIBE_CONFIGS);
    KafkaAction idempotentWriteAction = factory.getActionByName(KafkaActionConstant.IDEMPOTENT_WRITE);
    KafkaAction allAction = factory.getActionByName(KafkaActionConstant.ALL);

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
    assertEquals(Lists.newArrayList(alterConfigsAction),
        factory.getActionsByCode(alterConfigsAction.getActionCode()));
    assertEquals(Lists.newArrayList(describeConfigsAction),
            factory.getActionsByCode(describeConfigsAction.getActionCode()));
    assertEquals(Lists.newArrayList(idempotentWriteAction),
            factory.getActionsByCode(idempotentWriteAction.getActionCode()));
    assertEquals(Lists.newArrayList(readAction, writeAction, createAction, deleteAction,
        alterAction, describeAction, adminAction,
        alterConfigsAction, describeConfigsAction, idempotentWriteAction), factory.getActionsByCode(allAction
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
