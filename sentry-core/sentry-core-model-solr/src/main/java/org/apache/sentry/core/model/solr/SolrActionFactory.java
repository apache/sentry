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
package org.apache.sentry.core.model.solr;

import java.util.List;

import org.apache.sentry.core.common.BitFieldAction;
import org.apache.sentry.core.common.BitFieldActionFactory;

import com.google.common.collect.Lists;

public class SolrActionFactory extends BitFieldActionFactory {
  public enum SolrAction {
    UPDATE(SolrConstants.UPDATE, 0x0001),
    QUERY(SolrConstants.QUERY, 0x0002),
    ALL(SolrConstants.ALL, 0x0001|0x0002);

    private String name;
    private int code;
    private SolrAction(String name, int code) {
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

  public static class SolrBitFieldAction extends BitFieldAction {
    public SolrBitFieldAction(SolrAction action) {
      super(action.getName(), action.getCode());
    }
  }

  private final static SolrAction[] AllActions = SolrAction.values();
  /**
   * One bit set action array, includes UPDATE and QUERY
   */
  private final static SolrAction[] OneBitActions = new SolrAction[]{SolrAction.UPDATE, SolrAction.QUERY};

  @Override
  public List<? extends BitFieldAction> getActionsByCode(int actionCode) {
    List<SolrBitFieldAction> actions = Lists.newArrayList();
    for (SolrAction action : OneBitActions) {
      if ((action.code & actionCode) == action.code) {
        actions.add(new SolrBitFieldAction(action));
      }
    }
    return actions;
  }

  @Override
  public BitFieldAction getActionByName(String name) {
    SolrBitFieldAction val = null;
    for (SolrAction action : AllActions) {
      if (action.name.equalsIgnoreCase(name)) {
        return new SolrBitFieldAction(action);
      }
    }
    return val;
  }
}
