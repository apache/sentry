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
package org.apache.sentry.provider.db.service.persistent;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.StringReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import org.junit.Test;

public class TestSentryUpgradeOrder {
  @Test
  public void testReadUpgradeOrderGraphInNormalOrder() throws IOException {
    List<String> normalOrder = Arrays.asList(
      "1.7.0-to-1.8.0",
      "1.8.0-to-2.0.0",
      "2.0.0-to-2.1.0"
    );

    Map<String, List<String>> upgradeGraph =
      SentryUpgradeOrder.readUpgradeGraph(new StringReader(StringUtils.join(normalOrder, "\n")));

    // The returned graph should be in the following form:
    //   key: 1.7.0  value: 1.8.0
    //   key: 1.8.0  value: 2.0.0
    //   key: 2.0.0  value: 2.1.0
    assertEquals(3, upgradeGraph.size());
    assertEquals(Arrays.asList("1.8.0"), upgradeGraph.get("1.7.0"));
    assertEquals(Arrays.asList("2.0.0"), upgradeGraph.get("1.8.0"));
    assertEquals(Arrays.asList("2.1.0"), upgradeGraph.get("2.0.0"));
  }

  @Test
  public void testReadUpgradeOrderGraphInReverseOrder() throws IOException {
    List<String> simpleOrder = Arrays.asList(
      "2.0.0-to-2.1.0",
      "1.8.0-to-2.0.0",
      "1.7.0-to-1.8.0"
    );

    Map<String, List<String>> upgradeGraph =
      SentryUpgradeOrder.readUpgradeGraph(new StringReader(StringUtils.join(simpleOrder, "\n")));

    // The returned graph should be in the following form:
    //   key: 1.7.0  value: 1.8.0
    //   key: 1.8.0  value: 2.0.0
    //   key: 2.0.0  value: 2.1.0
    assertEquals(3, upgradeGraph.size());
    assertEquals(Arrays.asList("1.8.0"), upgradeGraph.get("1.7.0"));
    assertEquals(Arrays.asList("2.0.0"), upgradeGraph.get("1.8.0"));
    assertEquals(Arrays.asList("2.1.0"), upgradeGraph.get("2.0.0"));
  }

  @Test
  public void testReadUpgradeOrderGraphWithSameSourceVersions() throws IOException {
    List<String> normalOrder = Arrays.asList(
      "1.7.0-to-1.8.0",
      "1.7.0-to-2.0.0",
      "1.8.0-to-2.0.0",
      "2.0.0-to-2.1.0",
      "2.0.0-to-2.2.0"
    );

    Map<String, List<String>> upgradeGraph =
      SentryUpgradeOrder.readUpgradeGraph(new StringReader(StringUtils.join(normalOrder, "\n")));

    // The returned graph should be in the following form:
    //   key: 1.7.0  value: 1.8.0, 2.0.0
    //   key: 1.8.0  value: 2.0.0
    //   key: 2.0.0  value: 2.1.0, 2.2.0
    assertEquals(3, upgradeGraph.size());
    assertEquals(Arrays.asList("1.8.0", "2.0.0"), upgradeGraph.get("1.7.0"));
    assertEquals(Arrays.asList("2.0.0"), upgradeGraph.get("1.8.0"));
    assertEquals(Arrays.asList("2.1.0", "2.2.0"), upgradeGraph.get("2.0.0"));
  }

  @Test
  public void testReadUpgradeOrderGraphWithIgnoredVersions() throws IOException {
    List<String> simpleOrder = Arrays.asList(
      "# comment",       // comments are ignored
      "# 1.8.0-to-1.9.0",// comments are ignored
      "2.0.0-to-2.0.0",  // same source/target versions are ignored
      "1.8.0-to-",       // no target versions are ignored
      "-to-1.8.0",       // no source versions are ignored
      "1.8.0-1.7.0",     // versions without -to- are ignored
      "1.8.0-to-1.7.0-to-2.0.0",  // more than two versions are ignored
      "1.7.0-to-1.8.0"
    );

    Map<String, List<String>> upgradeGraph =
      SentryUpgradeOrder.readUpgradeGraph(new StringReader(StringUtils.join(simpleOrder, "\n")));

    // The returned graph should be in the following form:
    //   key: 1.7.0  value: 1.8.0
    assertEquals(1, upgradeGraph.size());
    assertEquals(Arrays.asList("1.8.0"), upgradeGraph.get("1.7.0"));
  }

  @Test
  public void testGetUpgradePathUsingSingleSourceVersions() {
    Map<String, List<String>> normalOrder = new HashMap<String, List<String>>() {{
      put("1.7.0", Arrays.asList("1.8.0"));
      put("1.8.0", Arrays.asList("2.0.0"));
      put("2.0.0", Arrays.asList("2.1.0"));
    }};

    List<String> upgradePath;

    // The returned upgrade path should be: 1.8.0 -> 2.0.0 -> 2.1.0
    upgradePath = SentryUpgradeOrder.getUpgradePath(normalOrder, "1.8.0", "2.1.0");
    assertEquals(2, upgradePath.size());
    assertEquals("1.8.0-to-2.0.0", upgradePath.get(0));
    assertEquals("2.0.0-to-2.1.0", upgradePath.get(1));

    // The returned upgrade path should be: 1.7.0 -> 1.8.0 -> 2.0.0
    upgradePath = SentryUpgradeOrder.getUpgradePath(normalOrder, "1.7.0", "2.0.0");
    assertEquals(2, upgradePath.size());
    assertEquals("1.7.0-to-1.8.0", upgradePath.get(0));
    assertEquals("1.8.0-to-2.0.0", upgradePath.get(1));

    // This upgrade path does not exist
    upgradePath = SentryUpgradeOrder.getUpgradePath(normalOrder, "1.8.0", "3.1.0");
    assertEquals(0, upgradePath.size());

    // This upgrade path does not exist
    upgradePath = SentryUpgradeOrder.getUpgradePath(normalOrder, "1.5.0", "2.1.0");
    assertEquals(0, upgradePath.size());
  }

  @Test
  public void testGetUpgradePathUsingMultipleSourceTargetVersions() {
    Map<String, List<String>> multiVersionsOrder = new HashMap<String, List<String>>() {{
      put("1.7.0", Arrays.asList("1.8.0", "2.0.0"));
      put("1.8.0", Arrays.asList("2.0.0"));
      put("1.9.0", Arrays.asList("2.1.0"));
      put("2.0.0", Arrays.asList("2.1.0"));
    }};

    List<String> upgradePath;

    // The returned upgrade path should be: 1.8.0 -> 2.0.0 -> 2.1.0
    upgradePath = SentryUpgradeOrder.getUpgradePath(multiVersionsOrder, "1.8.0", "2.1.0");
    assertEquals(2, upgradePath.size());
    assertEquals("1.8.0-to-2.0.0", upgradePath.get(0));
    assertEquals("2.0.0-to-2.1.0", upgradePath.get(1));

    // The returned upgrade path should be: 1.7.0 -> 1.8.0 -> 2.0.0
    upgradePath = SentryUpgradeOrder.getUpgradePath(multiVersionsOrder, "1.7.0", "2.0.0");
    assertEquals(2, upgradePath.size());
    assertEquals("1.7.0-to-1.8.0", upgradePath.get(0));
    assertEquals("1.8.0-to-2.0.0", upgradePath.get(1));

    // The returned upgrade path should be: 1.9.0 -> 2.1.0
    upgradePath = SentryUpgradeOrder.getUpgradePath(multiVersionsOrder, "1.9.0", "2.1.0");
    assertEquals(1, upgradePath.size());

    // The returned upgrade path should be: 1.7.0 -> 1.8.0 -> 2.0.0
    upgradePath = SentryUpgradeOrder.getUpgradePath(multiVersionsOrder, "1.7.0", "2.0.0");
    assertEquals(2, upgradePath.size());
    assertEquals("1.7.0-to-1.8.0", upgradePath.get(0));
    assertEquals("1.8.0-to-2.0.0", upgradePath.get(1));

    // This upgrade path does not exist
    upgradePath = SentryUpgradeOrder.getUpgradePath(multiVersionsOrder, "1.9.0", "2.0.0");
    assertEquals(0, upgradePath.size());
  }
}
