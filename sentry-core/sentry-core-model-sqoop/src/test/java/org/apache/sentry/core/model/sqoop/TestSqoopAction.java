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
package org.apache.sentry.core.model.sqoop;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;

import org.apache.sentry.core.model.sqoop.SqoopActionFactory.SqoopAction;
import org.junit.Test;

import com.google.common.collect.Lists;

public class TestSqoopAction {
  private SqoopActionFactory factory = new SqoopActionFactory();

  @Test
  public void testImpliesAction() {
    SqoopAction readAction = (SqoopAction)factory.getActionByName(SqoopActionConstant.READ);
    SqoopAction writeAction = (SqoopAction)factory.getActionByName(SqoopActionConstant.WRITE);
    SqoopAction allAction = (SqoopAction)factory.getActionByName(SqoopActionConstant.ALL);
    SqoopAction allNameAction = (SqoopAction)factory.getActionByName(SqoopActionConstant.ALL_NAME);

    assertTrue(allAction.implies(readAction));
    assertTrue(allAction.implies(writeAction));
    assertTrue(allAction.implies(allAction));

    assertTrue(readAction.implies(readAction));
    assertFalse(readAction.implies(writeAction));
    assertFalse(readAction.implies(allAction));

    assertTrue(writeAction.implies(writeAction));
    assertFalse(writeAction.implies(readAction));
    assertFalse(writeAction.implies(allAction));

    assertTrue(allNameAction.implies(readAction));
    assertTrue(allNameAction.implies(writeAction));
    assertTrue(allNameAction.implies(allAction));
  }

  @Test
  public void testGetActionByName() throws Exception {
    SqoopAction readAction = (SqoopAction)factory.getActionByName(SqoopActionConstant.READ);
    SqoopAction writeAction = (SqoopAction)factory.getActionByName(SqoopActionConstant.WRITE);
    SqoopAction allAction = (SqoopAction)factory.getActionByName(SqoopActionConstant.ALL);
    SqoopAction allNameAction = (SqoopAction)factory.getActionByName(SqoopActionConstant.ALL_NAME);

    assertTrue(readAction.equals(new SqoopAction(SqoopActionConstant.READ)));
    assertTrue(writeAction.equals(new SqoopAction(SqoopActionConstant.WRITE)));
    assertTrue(allAction.equals(new SqoopAction(SqoopActionConstant.ALL)));
    assertTrue(allNameAction.equals(new SqoopAction(SqoopActionConstant.ALL)));
  }

  @Test
  public void testGetActionsByCode() throws Exception {
    SqoopAction readAction = new SqoopAction(SqoopActionConstant.READ);
    SqoopAction writeAction = new SqoopAction(SqoopActionConstant.WRITE);
    SqoopAction allAction = new SqoopAction(SqoopActionConstant.ALL);

    assertEquals(Lists.newArrayList(readAction, writeAction), factory.getActionsByCode(allAction.getActionCode()));
    assertEquals(Lists.newArrayList(readAction), factory.getActionsByCode(readAction.getActionCode()));
    assertEquals(Lists.newArrayList(writeAction), factory.getActionsByCode(writeAction.getActionCode()));
  }
}
