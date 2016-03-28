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
package org.apache.sentry.policy.common;

import org.apache.sentry.core.common.*;
import org.apache.sentry.core.model.db.DBModelAuthorizable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ModelForTest implements Model {

  private Map<String, ImplyMethodType> implyMethodMap;
  private BitFieldActionFactory bitFieldActionFactory;

  public ModelForTest() {
    implyMethodMap = new HashMap<String, ImplyMethodType>();
    bitFieldActionFactory = new ActionFactoryForTest();

    implyMethodMap.put(DBModelAuthorizable.AuthorizableType.Server.name().toLowerCase(), ImplyMethodType.STRING);
    implyMethodMap.put(DBModelAuthorizable.AuthorizableType.Db.name().toLowerCase(), ImplyMethodType.STRING);
    implyMethodMap.put(DBModelAuthorizable.AuthorizableType.Table.name().toLowerCase(), ImplyMethodType.STRING);
    implyMethodMap.put(DBModelAuthorizable.AuthorizableType.Column.name().toLowerCase(), ImplyMethodType.STRING_CASE_SENSITIVE);
    implyMethodMap.put(DBModelAuthorizable.AuthorizableType.URI.name().toLowerCase(), ImplyMethodType.URL);
  }

  public Map<String, ImplyMethodType> getImplyMethodMap() {
    return implyMethodMap;
  }

  public BitFieldActionFactory getBitFieldActionFactory() {
    return bitFieldActionFactory;
  }

  public static class ActionFactoryForTest extends BitFieldActionFactory {
    enum ActionType {
      SELECT("select", 1),
      INSERT("insert", 2),
      ALL("all", SELECT.getCode() | INSERT.getCode()),
      ALL_STAR("*", SELECT.getCode() | INSERT.getCode());

      private String name;
      private int code;

      ActionType(String name, int code) {
        this.name = name;
        this.code = code;
      }

      public int getCode() {
        return code;
      }

      public String getName() {
        return name;
      }
    }

    public List<? extends BitFieldAction> getActionsByCode(int actionCode) {
      return null;
    }

    public BitFieldAction getActionByName(String name) {
      for (ActionType action : ActionType.values()) {
        if (action.name.equalsIgnoreCase(name)) {
          return new BitFieldAction(action.getName(), action.getCode());
        }
      }
      return null;
    }
  }
}
