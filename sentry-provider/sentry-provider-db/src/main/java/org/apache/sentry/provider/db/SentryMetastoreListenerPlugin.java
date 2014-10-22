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
package org.apache.sentry.provider.db;

import java.util.LinkedList;
import java.util.List;

/**
 * Plugin interface providing hooks to implementing classes, which are invoked
 * on path creation/updation and deletion
 */
public abstract class SentryMetastoreListenerPlugin {
  
  private static List<SentryMetastoreListenerPlugin> registry = new LinkedList<SentryMetastoreListenerPlugin>();
  
  public static void addToRegistry(SentryMetastoreListenerPlugin plugin) {
    registry.add(plugin);
  }

  public static List<SentryMetastoreListenerPlugin> getPlugins() {
    return registry;
  }

  public abstract void renameAuthzObject(String oldName, String oldPath,
      String newName, String newPath);
  
  public abstract void addPath(String authzObj, String path);

  public abstract void removePath(String authzObj, String path);

  public abstract void removeAllPaths(String authzObj);

}
