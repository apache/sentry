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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SentryUpgradeOrder {
  private static final Logger LOGGER = LoggerFactory.getLogger(SentryUpgradeOrder.class);

  /**
   * Returns a directed graph in the form of a map structure that contains the allowed upgrade
   * order schema versions read from a specific Reader.
   * </p>
   * The format of the upgrade order schema versions is as follows:
   *    SOURCE-to-TARGET
   * </p>
   * where SOURCE is the version from where to upgrade, and TARGET is the target version where
   * to upgrade, i.e.
   *   1.8.0-to-1.9.0
   * </p>
   * The reader may contain a list of upgrade path versions separated by a new line.
   *   1.8.0-to-1.9.0
   *   1.8.0-to-2.0.0
   *   1.9.0-to-2.1.0
   * </p>
   * Lines starting with the # are considered comments and are ignored.
   *
   * @param reader A Reader object from where to read the list of upgrade path versions.
   * @return A map structure that contains the directed graph.
   * @throws IOException If an error occurs while reading data from the Reader object.
   */
  public static Map<String, List<String>> readUpgradeGraph(Reader reader) throws IOException {
    Map<String, List<String>> upgradeSchemaVersions = new HashMap<>();

    try (BufferedReader bfReader = new BufferedReader(reader)) {
      String schemaVersion;
      while ((schemaVersion = bfReader.readLine()) != null) {
        String[] versions = schemaVersion.toLowerCase().split("-to-");

        // Valid upgrade versions has a source and a target version
        if (versions.length != 2) {
          LOGGER.warn("Ignoring unknown Sentry schema upgrade path: " + schemaVersion);
          continue;
        }

        String source = versions[0].trim();
        String target = versions[1].trim();

        // Valid upgrade versions should have different source and target versions
        if (source.isEmpty() || source.startsWith("#") || source.equals(target)) {
          continue;
        }

        // There could be multiple source versions with different targets.
        List<String> toVersions = upgradeSchemaVersions.getOrDefault(source, new LinkedList<>());
        toVersions.add(target);
        upgradeSchemaVersions.put(source, toVersions);
      }
    }

    return upgradeSchemaVersions;
  }

  /**
   * Returns a list of upgrade path versions sorted from lower version to higher version.
   *
   * @param upgrades The map structured that contains a graph of directed path upgrades.
   * @param from A string of the source version to search.
   * @param to A string of the target version to search.
   * @return An ordered list from lower to higher versions of the upgrade path versions.
   */
  public static List<String> getUpgradePath(Map<String, List<String>> upgrades, String from, String to) {
    // LinkedList is used to keep the correct order in Java 8 (ArrayList is known to not preserve
    // the order at the moment of insertion).
    List<String> upgradeListPath = new LinkedList<>();

    Stack<String> upgradePaths = new Stack<>();
    Set<String> visited = new HashSet<>();
    searchPath(upgrades, from, to, upgradePaths, visited);

    // Reverse the stack to put the correct order in the list.
    // To keep the behavior correctly, a list is returned instead of the stack to avoid the
    // incorrect order obtained if a stack is used in the for() loop.
    while (!upgradePaths.isEmpty()) {
      upgradeListPath.add(upgradePaths.pop());
    }

    return upgradeListPath;
  }

  /**
   * Searches the complete upgrade path from the list of valid upgrades.
   *
   * @param from The string version from where to upgrade.
   * @param to The string version to upgrade.
   * @param upgradePath A stack where to put the complete upgrade path. Empty if no upgrade path
   *                    is found.
   * @param visited A set where to track the visited upgrade paths.
   *
   * @return True if a complete upgrade path is found; False otherwise.
   */
  private static boolean searchPath(Map<String, List<String>> graph, String from, String to, Stack<String> upgradePath, Set<String> visited) {
    /*
     * It is not necessary to find the shortest path to the upgrade as the number of upgrade
     * scripts are small and do not cause too much overhead during an upgrade. So a simple depth-first
     * search is done to find the requested path.
     */

    if (from.equals(to)) {
      return true;
    }

    if (!graph.containsKey(from)) {
      return false;
    }

    boolean found = false;
    for (String toVersion : graph.get(from)) {
      String nextPath = generateUpgradeVersionString(from, toVersion);
      boolean isVisited = visited.contains(nextPath);
      if (!isVisited) {
        visited.add(nextPath);
        found = searchPath(graph, toVersion, to, upgradePath, visited);
        if (found) {
          upgradePath.push(nextPath);
          break;
        } else {
          visited.remove(nextPath);
        }
      }
    }

    return found;
  }

  private static String generateUpgradeVersionString(String from, String to) {
    StringBuilder sb = new StringBuilder();
    sb.append(from).append("-to-").append(to);
    return sb.toString();
  }
}
