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
package org.apache.sentry.core.model.search;

import java.util.List;

import org.apache.sentry.core.common.BitFieldAction;
import org.apache.sentry.core.common.BitFieldActionFactory;

import com.google.common.collect.Lists;

public class SearchActionFactory extends BitFieldActionFactory {
  public enum SearchAction {
    UPDATE(SearchConstants.UPDATE, 0x0001),
    QUERY(SearchConstants.QUERY, 0x0002),
    ALL(SearchConstants.ALL, 0x0001|0x0002);

    private String name;
    private int code;
    private SearchAction(String name, int code) {
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

  public static class SearchBitFieldAction extends BitFieldAction {
    public SearchBitFieldAction(SearchAction action) {
      super(action.getName(), action.getCode());
    }
  }

  private final static SearchAction[] AllActions = SearchAction.values();
  /**
   * One bit set action array, includes UPDATE and QUERY
   */
  private final static SearchAction[] OneBitActions = new SearchAction[]{SearchAction.UPDATE, SearchAction.QUERY};

  @Override
  public List<? extends BitFieldAction> getActionsByCode(int actionCode) {
    List<SearchBitFieldAction> actions = Lists.newArrayList();
    for (SearchAction action : OneBitActions) {
      if ((action.code & actionCode) == action.code) {
        actions.add(new SearchBitFieldAction(action));
      }
    }
    return actions;
  }

  @Override
  public BitFieldAction getActionByName(String name) {
    SearchBitFieldAction val = null;
    for (SearchAction action : AllActions) {
      if (action.name.equalsIgnoreCase(name)) {
        return new SearchBitFieldAction(action);
      }
    }
    return val;
  }
}
