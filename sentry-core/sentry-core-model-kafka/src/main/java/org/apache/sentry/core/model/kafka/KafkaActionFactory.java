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

import java.util.Arrays;
import java.util.List;

import org.apache.sentry.core.common.BitFieldAction;
import org.apache.sentry.core.common.BitFieldActionFactory;

import com.google.common.collect.Lists;

/**
 * Factory for creating actions supported by Kafka.
 */
public class KafkaActionFactory extends BitFieldActionFactory {
  private static KafkaActionFactory instance;
  private KafkaActionFactory() {}

  /**
   * Get instance of KafkaActionFactory, which is a singleton.
   *
   * @return Instance of KafkaActionFactory.
   */
  public static KafkaActionFactory getInstance() {
    if (instance == null) {
      instance = new KafkaActionFactory();
    }

    return instance;
  }

  /**
   * Types of actions supported by Kafka.
   */
  public enum KafkaActionType {
    READ(KafkaActionConstant.READ, 1),
    WRITE(KafkaActionConstant.WRITE, 2),
    CREATE(KafkaActionConstant.CREATE, 4),
    DELETE(KafkaActionConstant.DELETE, 8),
    ALTER(KafkaActionConstant.ALTER, 16),
    DESCRIBE(KafkaActionConstant.DESCRIBE, 32),
    CLUSTERACTION(KafkaActionConstant.CLUSTER_ACTION, 64),
    ALTERCONFIGS(KafkaActionConstant.ALTER_CONFIGS, 128),
    DESCRIBECONFIGS(KafkaActionConstant.DESCRIBE_CONFIGS, 256),
    IDEMPOTENTWRITE(KafkaActionConstant.IDEMPOTENT_WRITE, 512),
    ALL(KafkaActionConstant.ALL, READ.getCode() | WRITE.getCode() | CREATE.getCode()
        | DELETE.getCode() | ALTER.getCode()| DESCRIBE.getCode() | CLUSTERACTION.getCode()
        | ALTERCONFIGS.getCode() | DESCRIBECONFIGS.getCode() | IDEMPOTENTWRITE.getCode());

    private String name;
    private int code;

    /**
     * Create Kafka action type based on provided kafkaAction and code.
     *
     * @param name Name of Kafka action.
     * @param code Integer representation of Kafka action's code.
     */
    KafkaActionType(String name, int code) {
      this.name = name;
      this.code = code;
    }

    /**
     * Get code for this Kafka's action.
     *
     * @return Code for this Kafka's action.
     */
    public int getCode() {
      return code;
    }

    /**
     * Get kafkaAction of this Kafka's action.
     *
     * @return Name of this Kafka's action.
     */
    public String getName() {
      return name;
    }

    /**
     * Check if Kafka action type with {@code kafkaAction} as string representation exists.
     *
     * @param name String representation of a valid Kafka action type.
     * @return If Kafka action type with {@code kafkaAction} as string representation exists.
     */
    static boolean hasActionType(String name) {
      for (KafkaActionType action : KafkaActionType.values()) {
        if (action.name.equalsIgnoreCase(name)) {
          return true;
        }
      }
      return false;
    }

    /**
     * Create Kafka's action of type provided as kafkaAction.
     *
     * @param name String representation of Kafka's action type.
     * @return Kafka's action type based on provided kafkaAction, if such action type is found, else null.
     */
    static KafkaActionType getActionByName(String name) {
      for (KafkaActionType action : KafkaActionType.values()) {
        if (action.name.equalsIgnoreCase(name)) {
          return action;
        }
      }
      return null; // Can't get ActionType of provided kafkaAction
    }

    /**
     * Create Kafka's action types represented by provided code.
     *
     * @param code Integer representation of Kafka's action types.
     * @return List of Kafka's action types represented by provided code, if none action types are found return an empty list.
     */
    static List<KafkaActionType> getActionByCode(int code) {
      List<KafkaActionType> actions = Lists.newArrayList();
      for (KafkaActionType action : KafkaActionType.values()) {
        if (((action.code & code) == action.code) && (action != KafkaActionType.ALL)) {
          // KafkaActionType.ALL action should not return in the list
          actions.add(action);
        }
      }
      if (actions.isEmpty()) {
        return Arrays.asList();
      }
      return actions;
    }
  }

  /**
   * Kafka Action
   */
  public static class KafkaAction extends BitFieldAction {
    /**
     * Create Kafka action based on provided kafkaAction.
     *
     * @param name Name of Kafka action.
     */
    public KafkaAction(String name) {
      this(KafkaActionType.getActionByName(name));
    }

    /**
     * Create Kafka action based on provided Kafka action type.
     *
     * @param actionType Type of Kafka action for which action has to be created.
     */
    public KafkaAction(KafkaActionType actionType) {
      super(actionType.name(), actionType.getCode());
    }
  }

  /**
   * Get Kafka actions represented by provided action code.
   *
   * @param actionCode Integer code for required Kafka actions.
   * @return List of Kafka actions represented by provided action code.
   */
  @Override
  public List<KafkaAction> getActionsByCode(int actionCode) {
    List<KafkaAction> actions = Lists.newArrayList();
    for (KafkaActionType action : KafkaActionType.getActionByCode(actionCode)) {
      actions.add(new KafkaAction(action));
    }
    return actions;
  }

  /**
   * Get Kafka action represented by provided action kafkaAction.
   *
   * @param name String representation of required action kafkaAction.
   * @return Kafka action represented by provided action kafkaAction.
   */
  @Override
  public KafkaAction getActionByName(String name) {
    if (name.equalsIgnoreCase("*")) {
      return new KafkaAction("ALL");
    }
    return KafkaActionType.hasActionType(name) ? new KafkaAction(name) : null;
  }
}
