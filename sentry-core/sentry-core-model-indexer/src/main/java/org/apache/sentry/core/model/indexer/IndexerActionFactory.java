/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sentry.core.model.indexer;

import java.util.List;

import org.apache.sentry.core.common.BitFieldAction;
import org.apache.sentry.core.common.BitFieldActionFactory;

import com.google.common.collect.Lists;

public class IndexerActionFactory extends BitFieldActionFactory {
  public enum IndexerAction {
    WRITE(IndexerConstants.WRITE, 0x0001),
    READ(IndexerConstants.READ, 0x0002),
    ALL(IndexerConstants.ALL, 0x0001|0x0002);

    private String name;
    private int code;
    private IndexerAction(String name, int code) {
      this.name = name;
      this.code = code;
    }
    public String getName() {
      return name;
    }
    public int getCode() {
      return code;
    }
  }

  public static class IndexerBitFieldAction extends BitFieldAction {
    public IndexerBitFieldAction(IndexerAction action) {
      super(action.getName(), action.getCode());
    }
  }

  private final static IndexerAction[] AllActions = IndexerAction.values();
  /**
   * One bit set action array
   */
  private final static IndexerAction[] OneBitActions = new IndexerAction[]{IndexerAction.WRITE, IndexerAction.READ};

  @Override
  public List<? extends BitFieldAction> getActionsByCode(int actionCode) {
    List<IndexerBitFieldAction> actions = Lists.newArrayList();
    for (IndexerAction action : OneBitActions) {
      if ((action.code & actionCode) == action.code) {
        actions.add(new IndexerBitFieldAction(action));
      }
    }
    return actions;
  }

  @Override
  public BitFieldAction getActionByName(String name) {
    IndexerBitFieldAction val = null;
    for (IndexerAction action : AllActions) {
      if (action.name.equalsIgnoreCase(name)) {
        return new IndexerBitFieldAction(action);
      }
    }
    return val;
  }
}
