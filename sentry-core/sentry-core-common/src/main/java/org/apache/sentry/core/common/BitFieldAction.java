/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements. See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package org.apache.sentry.core.common;
/**
 * Represents a bit set action in the authorize model.Take Solr component for
 * example, There exists three actions, UPDATE, QUERY and ALL.
 * The a bit set for UPDATE is 0x0001, QUERY is 0x0002, ALL is 0x0001|0x0002=0x0003
 */
public abstract class BitFieldAction implements Action {
  private String name;
  private int code;

  public BitFieldAction(String name, int code) {
    this.name = name;
    this.code = code;
  }

  public int getActionCode() {
    return code;
  }
  /**
   * Return true if this action implies that action.
   * @param that
   */
  public boolean implies(BitFieldAction that) {
    if (that != null) {
      return (code & that.code) == that.code;
    }
    return false;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof BitFieldAction)) {
      return false;
    }
    BitFieldAction that = (BitFieldAction)obj;
    return (code == that.code) && (name.equalsIgnoreCase(that.name));
  }

  @Override
  public int hashCode() {
    return code + name.hashCode();
  }

  @Override
  public String toString() {
    return name;
  }

  @Override
  public String getValue() {
    return name;
  }
}
