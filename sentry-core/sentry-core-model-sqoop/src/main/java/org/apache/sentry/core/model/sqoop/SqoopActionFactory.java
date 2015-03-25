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

import java.util.List;

import org.apache.sentry.core.common.BitFieldAction;
import org.apache.sentry.core.common.BitFieldActionFactory;

import com.google.common.collect.Lists;

public class SqoopActionFactory extends BitFieldActionFactory {
  enum SqoopActionType {
    READ(SqoopActionConstant.READ,1),
    WRITE(SqoopActionConstant.WRITE,2),
    ALL(SqoopActionConstant.ALL,READ.getCode() | WRITE.getCode());

    private String name;
    private int code;
    SqoopActionType(String name, int code) {
      this.name = name;
      this.code = code;
    }

    public int getCode() {
      return code;
    }

    public String getName() {
      return name;
    }

    static SqoopActionType getActionByName(String name) {
      for (SqoopActionType action : SqoopActionType.values()) {
        if (action.name.equalsIgnoreCase(name)) {
          return action;
        }
      }
      throw new RuntimeException("can't get sqoopActionType by name:" + name);
    }

    static List<SqoopActionType> getActionByCode(int code) {
      List<SqoopActionType> actions = Lists.newArrayList();
      for (SqoopActionType action : SqoopActionType.values()) {
        if (((action.code & code) == action.code ) &&
            (action != SqoopActionType.ALL)) {
          //SqoopActionType.ALL action should not return in the list
          actions.add(action);
        }
      }
      if (actions.isEmpty()) {
        throw new RuntimeException("can't get sqoopActionType by code:" + code);
      }
      return actions;
    }
  }

  public static class SqoopAction extends BitFieldAction {
    public SqoopAction(String name) {
      this(SqoopActionType.getActionByName(name));
    }
    public SqoopAction(SqoopActionType sqoopActionType) {
      super(sqoopActionType.name, sqoopActionType.code);
    }
  }

  @Override
  public BitFieldAction getActionByName(String name) {
    //Check the name is All
    if (SqoopActionConstant.ALL_NAME.equalsIgnoreCase(name)) {
      return new SqoopAction(SqoopActionType.ALL);
    }
    return new SqoopAction(name);
  }

  @Override
  public List<? extends BitFieldAction> getActionsByCode(int code) {
    List<SqoopAction> actions = Lists.newArrayList();
    for (SqoopActionType action : SqoopActionType.getActionByCode(code)) {
      actions.add(new SqoopAction(action));
    }
    return actions;
  }
}
