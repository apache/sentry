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

package org.apache.sentry.core.common.utils;

import static org.apache.sentry.core.common.utils.SentryConstants.NULL_COL;

import com.google.common.base.Strings;
import java.util.ArrayList;
import java.util.List;

/**
 * Classes to hold general utility functions that could be used across Sentry
 */
public final class SentryUtils {

  /**
   * Given a list of sorted numbers, return the string that prints in the collapsed format.
   * <p>
   * e.g:
   * <li> Input: [1, 2, 3, 5, 7] </li>
   * <li> Output: "[1-3, 5, 7]" </li>
   * </p>
   * @param nums List of sorted numbers
   * @return Collapsed string representation of the numbers
   */
  public static String collapseNumsToString(List<Long> nums) {
    List<String> collapsedStrings = new ArrayList<>();

    // Using startIndex, movingIndex we maintain a variable size moving window across the Array where at any
    // point in time, startIndex points to the beginning of a continuous sequence, movingIndex points to the last known
    // sequential number of that particular subsequence and currentGroupSize captures the size of the subsequence.
    int startIndex = 0;
    int movingIndex = 0;
    int currentGroupSize = 0;
    int listSize = nums.size();

    // Using the startIndex as terminating condition to capture the last sequence(before the list terminates)
    // within the loop itself to avoid code duplication.
    while (startIndex < listSize) {
      if (movingIndex == listSize ||
          nums.get(startIndex) + currentGroupSize != nums.get(movingIndex)) {
        long startID = nums.get(startIndex);
        long endID = nums.get(movingIndex - 1);
        if (startID == endID) {
          collapsedStrings.add(String.format("%d", startID));
        } else if (endID - startID == 1) {
          collapsedStrings.add(String.format("%d", startID));
          collapsedStrings.add(String.format("%d", endID));
        } else {
          collapsedStrings.add(String.format("%d-%d", startID, endID));
        }

        startIndex = movingIndex;
        currentGroupSize = 0;
        continue;
      }

      movingIndex++;
      currentGroupSize++;
    }

    return collapsedStrings.toString();
  }

  /**
   * Function to check if a string is null, empty or @NULL_COLL specifier
   * @param s string input, and can be null.
   * @return True if the input string represents a NULL string - when it is null, empty or equals @NULL_COL
   */
  public static boolean isNULL(String s) {
    return Strings.isNullOrEmpty(s) || s.equals(NULL_COL);
  }
}
