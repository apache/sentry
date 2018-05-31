/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 *     http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.sentry.provider.db.service.model;

import org.apache.sentry.core.common.utils.SentryUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Common utilities for model objects
 */
public final class MSentryUtil {
  /**
   * Intern a string but only if it is not null
   * @param arg String to be interned, may be null
   * @return interned string or null
   */
  static String safeIntern(String arg) {
    return (arg != null) ? arg.intern() : null;
  }

  /**
   * Given a collection of MSentryChange's, retrieve the change id's associated and return as a list
   * <p>
   * e.g:
   * <li> Input: [MSentryChange(1), MSentryChange(2), MSentryChange(3), MSentryChange(5), MSentryChange(7)] </li>
   * <li> Output: [1, 2, 3, 5 ,7] </li>
   * </p>
   * @param changes List of {@link MSentryChange}
   * @return List of changeID's
   */
  private static List<Long> getChangeIds(Collection<? extends MSentryChange> changes) {
    List<Long> ids = changes.isEmpty() ? Collections.<Long>emptyList() : new ArrayList<Long>(changes.size());
    for (MSentryChange change : changes) {
      ids.add(change.getChangeID());
    }
    return ids;
  }

  /**
   * Given a collection of MSentryChange instances sorted by ID return true if and only if IDs are sequential (do not contain holes)
   * <p>
   * e.g:
   * <li> Input: [MSentryChange(1), MSentryChange(2), MSentryChange(3), MSentryChange(5), MSentryChange(7)] </li>
   * <li> Output: False </li>
   * <li> Input: [MSentryChange(1), MSentryChange(2), MSentryChange(3), MSentryChange(4), MSentryChange(5)] </li>
   * <li> Output: True </li>
   * </p>
   * @param changes List of {@link MSentryChange}
   * @return True if all the ids are sequential otherwise returns False
   */
  public static boolean isConsecutive(List<? extends MSentryChange> changes) {
    int size = changes.size();
    return (size <= 1) || (changes.get(size - 1).getChangeID() - changes.get(0).getChangeID() + 1 == size);
  }

  /**
   * Given a collection of MSentryChange instances sorted by ID, return the string that prints in the collapsed format.
   * <p>
   * e.g:
   * <li> Input: [MSentryChange(1), MSentryChange(2), MSentryChange(3), MSentryChange(5), MSentryChange(7)] </li>
   * <li> Output: "[1-3, 5, 7]" </li>
   * </p>
   * @param changes  List of {@link MSentryChange}
   * @return Collapsed string representation of the changeIDs
   */
  public static String collapseChangeIDsToString(Collection<? extends MSentryChange> changes) {
    return SentryUtils.collapseNumsToString(getChangeIds(changes));
  }
}
