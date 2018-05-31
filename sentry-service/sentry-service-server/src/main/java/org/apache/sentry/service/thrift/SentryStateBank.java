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


import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.AtomicLongMap;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * <p>SentryStateBank is a state visibility manager to allow components to communicate state to other
 * parts of the application.</p>
 *
 * <p>It allows you to provide multiple boolean states for a component and expose those states to
 * other parts of the application without having references to the actual instances of the classes
 * setting those states.</p>
 *
 * <p>SentryStateBank uses a bitmasked long in order to store the states, so its very compact and
 * efficient.</p>
 *
 * <p>States are defined using an enum that implements the {@link SentryState} interface.  The
 * {@link SentryState} implementation can provide up to 64 states per components.  The {@link SentryState#getValue()}
 * implementation should return a bitshift of the oridinal of the enum value.  This gives the bitmask
 * location to be checking for the state.</p>
 *
 * <p>The following is an example of a simple {@link SentryState} enum implementation</p>
 *
 * <pre>
 * {@code
 *
 * public enum ExampleState implements SentryState {
 *  FIRST_STATE,
 *  SECOND_STATE;
 *
 *  public static final String COMPONENT = "ExampleState";
 *
 *  @Override
 *  public long getValue() {
 *    return 1 << this.ordinal();
 *  }
 * }
 * }
 * </pre>
 *
 * <p>This class is thread safe.  It uses a {@link ReentrantReadWriteLock} to wrap accesses and changes
 * to the state.</p>
 */
@ThreadSafe
public final class SentryStateBank {

  private static final Logger LOGGER = LoggerFactory.getLogger(SentryStateBank.class);
  private static final AtomicLongMap<String> states = AtomicLongMap.create();
  private static final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  protected SentryStateBank() {
  }

  @VisibleForTesting
  static void clearAllStates() {
    states.clear();
    LOGGER.debug("All states have been cleared.");
  }

  @VisibleForTesting
  static void resetComponentState(String component) {
    states.remove(component);
    LOGGER.debug("All states have been cleared for component {}", component);
  }

  /**
   * Enables a state
   *
   * @param component the component for the state
   * @param state the state to disable
   */
  public static void enableState(String component, SentryState state) {
    lock.writeLock().lock();
    try {
      states.put(component, states.get(component) | state.getValue());
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("{} entered state {}", component, state.toString());
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Disables a state for a component
   *
   * @param component the component for the state
   * @param state the state to disable
   */
  public static void disableState(String component, SentryState state) {
    lock.writeLock().lock();
    try {
      states.put(component, states.get(component) & (~state.getValue()));
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("{} exited state {}", component, state.toString());
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Returns if a state is enabled or not
   *
   * @param component The component for the state
   * @param state the SentryState to check
   * @return true if the state for the component is enabled
   */
  public static boolean isEnabled(String component, SentryState state) {
    lock.readLock().lock();
    try {
      return (states.get(component) & state.getValue()) == state.getValue();
    } finally {
      lock.readLock().unlock();
    }

  }

  /**
   * Checks if all of the states passed in are enabled
   *
   * @param component The component for the states
   * @param passedStates the SentryStates to check
   */
  public static boolean hasStatesEnabled(String component, Set<SentryState> passedStates) {
    lock.readLock().lock();
    try {
      long value = 0L;

      for (SentryState state : passedStates) {
          value += state.getValue();
      }
      return (states.get(component) & value) == value;
    } finally {
      lock.readLock().unlock();
    }
  }
}
