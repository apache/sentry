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

import java.util.List;

import org.apache.sentry.core.common.BitFieldAction;
import org.apache.sentry.core.common.BitFieldActionFactory;

import com.google.common.collect.Lists;

public class KafkaActionFactory extends BitFieldActionFactory {

  enum KafkaActionType {
    READ(KafkaActionConstant.READ, 1),
    WRITE(KafkaActionConstant.WRITE, 2),
    CREATE(KafkaActionConstant.CREATE, 4),
    DELETE(KafkaActionConstant.DELETE, 8),
    ALTER(KafkaActionConstant.ALTER, 16),
    DESCRIBE(KafkaActionConstant.DESCRIBE, 32),
    ADMIN(KafkaActionConstant.CLUSTER_ACTION, 64),
    ALL(KafkaActionConstant.ALL, READ.getCode() | WRITE.getCode() | CREATE.getCode()
        | DELETE.getCode() | ALTER.getCode()| DESCRIBE.getCode() | ADMIN.getCode());

    private String name;
    private int code;

    KafkaActionType(String name, int code) {
      this.name = name;
      this.code = code;
    }

    public int getCode() {
      return code;
    }

    public String getName() {
      return name;
    }

    static KafkaActionType getActionByName(String name) {
      for (KafkaActionType action : KafkaActionType.values()) {
        if (action.name.equalsIgnoreCase(name)) {
          return action;
        }
      }
      throw new RuntimeException("can't get ActionType by name:" + name);
    }

    static List<KafkaActionType> getActionByCode(int code) {
      List<KafkaActionType> actions = Lists.newArrayList();
      for (KafkaActionType action : KafkaActionType.values()) {
        if (((action.code & code) == action.code) && (action != KafkaActionType.ALL)) {
          // KafkaActionType.ALL action should not return in the list
          actions.add(action);
        }
      }
      if (actions.isEmpty()) {
        throw new RuntimeException("can't get ActionType by code:" + code);
      }
      return actions;
    }
  }

  public static class KafkaAction extends BitFieldAction {
    public KafkaAction(String name) {
      this(KafkaActionType.getActionByName(name));
    }

    public KafkaAction(KafkaActionType actionType) {
      super(actionType.name, actionType.code);
    }
  }

  @Override
  public List<KafkaAction> getActionsByCode(int actionCode) {
    List<KafkaAction> actions = Lists.newArrayList();
    for (KafkaActionType action : KafkaActionType.getActionByCode(actionCode)) {
      actions.add(new KafkaAction(action));
    }
    return actions;
  }

  @Override
  public KafkaAction getActionByName(String name) {
    // Check the name is All
    if (KafkaActionConstant.ALL_NAME.equalsIgnoreCase(name)) {
      return new KafkaAction(KafkaActionType.ALL);
    }
    return new KafkaAction(name);
  }

}
