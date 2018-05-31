/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements.  See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the License.  You may obtain
 * a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.sentry.service.thrift;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.HashSet;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class TestSentryStateBank {

  @Before
  public void setUp() {
    SentryStateBank.clearAllStates();
  }

  @Test
  public void testEnableState() {
    SentryStateBank.enableState(TestState.COMPONENT, TestState.FIRST_STATE);
    assertTrue("Expected FIRST_STATE to be enabled",
        SentryStateBank.isEnabled(TestState.COMPONENT, TestState.FIRST_STATE));
    assertFalse("Expected SECOND_STATE to be disabled",
        SentryStateBank.isEnabled(TestState.COMPONENT, TestState.SECOND_STATE));
  }

  @Test
  public void testStatesGetDisabled() {
    SentryStateBank.enableState(TestState.COMPONENT, TestState.FIRST_STATE);
    assertTrue("Expected FIRST_STATE to be enabled",
        SentryStateBank.isEnabled(TestState.COMPONENT, TestState.FIRST_STATE));
    SentryStateBank.disableState(TestState.COMPONENT, TestState.FIRST_STATE);
    assertFalse("Expected FIRST_STATE to be disabled",
        SentryStateBank.isEnabled(TestState.COMPONENT, TestState.FIRST_STATE));
  }

  @Test
  public void testCheckMultipleStateCheckSuccess() {
    SentryStateBank.enableState(TestState.COMPONENT, TestState.FIRST_STATE);
    SentryStateBank.enableState(TestState.COMPONENT, TestState.SECOND_STATE);

    assertTrue("Expected both FIRST_STATE and SECOND_STATE to be enabled",
        SentryStateBank.hasStatesEnabled(TestState.COMPONENT, new HashSet<SentryState>(
            Arrays.asList(TestState.FIRST_STATE, TestState.SECOND_STATE))));
  }

  @Test
  public void testCheckMultipleStateCheckFailure() {
    SentryStateBank.enableState(TestState.COMPONENT, TestState.FIRST_STATE);
    assertFalse("Expected only FIRST_STATE to be enabled",
        SentryStateBank.hasStatesEnabled(TestState.COMPONENT, new HashSet<SentryState>(
            Arrays.asList(TestState.FIRST_STATE, TestState.SECOND_STATE))));
  }


  public enum TestState implements SentryState {
    FIRST_STATE,
    SECOND_STATE;

    public static final String COMPONENT = "TestState";

    @Override
    public long getValue() {
      return 1 << this.ordinal();
    }
  }
}
