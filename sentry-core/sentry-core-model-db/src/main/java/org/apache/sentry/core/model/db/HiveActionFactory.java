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
package org.apache.sentry.core.model.db;

import org.apache.sentry.core.common.BitFieldAction;
import org.apache.sentry.core.common.BitFieldActionFactory;

import java.util.List;

// The class is used to define the privilege code for Hive
public class HiveActionFactory extends BitFieldActionFactory {

  enum ActionType {
    SELECT(AccessConstants.SELECT, 1),    // binary: 00000001
    INSERT(AccessConstants.INSERT, 2),    // binary: 00000010
    ALTER(AccessConstants.ALTER, 4),      // binary: 00000100
    CREATE(AccessConstants.CREATE, 8),    // binary: 00001000
    DROP(AccessConstants.DROP, 16),       // binary: 00010000
    INDEX(AccessConstants.INDEX, 32),     // binary: 00100000
    LOCK(AccessConstants.LOCK, 64),       // binary: 01000000

    // For the compatibility, ALL, ALL_STAR, SOME have the same binary value: 01111111
    // They have the different names which are "ALL", "*", "+"
    ALL(AccessConstants.ACTION_ALL, SELECT.getCode() | INSERT.getCode() | ALTER.getCode() | CREATE.getCode() |
            DROP.getCode() | INDEX.getCode() | LOCK.getCode()),   // binary: 01111111
    ALL_STAR(AccessConstants.ALL, ALL.getCode()),                 // binary: 01111111
    SOME(AccessConstants.SOME, ALL.getCode()),                    // binary: 01111111

    // OWNER is a special privilege that behaves like the ALL or ALL_STAR privilege.
    OWNER(AccessConstants.OWNER, ALL.getCode());                  // binary: 01111111

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