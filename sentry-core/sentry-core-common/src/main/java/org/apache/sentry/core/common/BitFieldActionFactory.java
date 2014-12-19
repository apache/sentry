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
package org.apache.sentry.core.common;

import java.util.List;

public abstract class BitFieldActionFactory {
  /**
   * Get BitFieldAction list by the given action code.
   * Take the Solr for example, the ALL action code is 0x0003, two bits are set.
   * The return BitFieldAction list are UPDATE action(0x0001) and QUERY action(0x0002)
   * @param actionCode
   * @return The BitFieldAction List
   */
  public abstract List<? extends BitFieldAction> getActionsByCode(int actionCode);
  /**
   * Get the BitFieldAction from the given name
   * @param name
   * @return
   */
  public abstract BitFieldAction getActionByName(String name);
}
