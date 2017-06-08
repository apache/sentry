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

package org.apache.sentry.provider.db.service.model;

import org.apache.sentry.hdfs.PathsUpdate;
import org.apache.sentry.hdfs.PermissionsUpdate;

import org.apache.sentry.hdfs.Updateable;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

public class TestMSentryUtil {

  @Test
  public void testMSentryUtilWithPathChanges() throws Exception {
    List<MSentryPathChange> changes = new ArrayList<>();
    PathsUpdate update = new PathsUpdate(1, false);

    changes.add(new MSentryPathChange(1, update));
    assertEquals("Collapsed string should match", "[1]",
        MSentryUtil.collapseChangeIDsToString(changes));
    assertTrue("List of changes should be consecutive", MSentryUtil.isConsecutive(changes));

    changes.add(new MSentryPathChange(2, update));
    assertEquals("Collapsed string should match", "[1, 2]",
        MSentryUtil.collapseChangeIDsToString(changes));
    assertTrue("List of changes should be consecutive", MSentryUtil.isConsecutive(changes));

    changes.add(new MSentryPathChange(4, update));
    assertEquals("Collapsed string should match", "[1, 2, 4]",
        MSentryUtil.collapseChangeIDsToString(changes));
    assertFalse("List of changes should not be consecutive", MSentryUtil.isConsecutive(changes));

    changes.add(new MSentryPathChange(5, update));
    assertEquals("Collapsed string should match", "[1, 2, 4, 5]",
        MSentryUtil.collapseChangeIDsToString(changes));
    assertFalse("List of changes should not be consecutive", MSentryUtil.isConsecutive(changes));

    changes.add(new MSentryPathChange(6, update));
    assertEquals("Collapsed string should match", "[1, 2, 4-6]",
        MSentryUtil.collapseChangeIDsToString(changes));
    assertFalse("List of changes should not be consecutive", MSentryUtil.isConsecutive(changes));

    changes.add(new MSentryPathChange(8, update));
    assertEquals("Collapsed string should match", "[1, 2, 4-6, 8]",
        MSentryUtil.collapseChangeIDsToString(changes));
    assertFalse("List of changes should not be consecutive", MSentryUtil.isConsecutive(changes));
  }

  @Test
  public void testMSentryUtilWithPermChanges() throws Exception {
    List<MSentryPermChange> changes = new ArrayList<>();
    PermissionsUpdate update = new PermissionsUpdate(1, false);

    changes.add(new MSentryPermChange(1, update));
    assertEquals("Collapsed string should match", "[1]",
        MSentryUtil.collapseChangeIDsToString(changes));
    assertTrue("List of changes should be consecutive", MSentryUtil.isConsecutive(changes));

    changes.add(new MSentryPermChange(2, update));
    assertEquals("Collapsed string should match", "[1, 2]",
        MSentryUtil.collapseChangeIDsToString(changes));
    assertTrue("List of changes should be consecutive", MSentryUtil.isConsecutive(changes));

    changes.add(new MSentryPermChange(4, update));
    assertEquals("Collapsed string should match", "[1, 2, 4]",
        MSentryUtil.collapseChangeIDsToString(changes));
    assertFalse("List of changes should not be consecutive", MSentryUtil.isConsecutive(changes));

    changes.add(new MSentryPermChange(5, update));
    assertEquals("Collapsed string should match", "[1, 2, 4, 5]",
        MSentryUtil.collapseChangeIDsToString(changes));
    assertFalse("List of changes should not be consecutive", MSentryUtil.isConsecutive(changes));

    changes.add(new MSentryPermChange(6, update));
    assertEquals("Collapsed string should match", "[1, 2, 4-6]",
        MSentryUtil.collapseChangeIDsToString(changes));
    assertFalse("List of changes should not be consecutive", MSentryUtil.isConsecutive(changes));

    changes.add(new MSentryPermChange(8, update));
    assertEquals("Collapsed string should match", "[1, 2, 4-6, 8]",
        MSentryUtil.collapseChangeIDsToString(changes));
    assertFalse("List of changes should not be consecutive", MSentryUtil.isConsecutive(changes));
  }
}
