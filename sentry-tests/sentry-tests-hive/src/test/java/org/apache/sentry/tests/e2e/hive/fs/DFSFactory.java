/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sentry.tests.e2e.hive.fs;

import java.io.File;

import org.apache.sentry.tests.e2e.hive.hiveserver.HiveServerFactory.HiveServer2Type;

import com.google.common.annotations.VisibleForTesting;

public class DFSFactory {
  public static DFS create(String dfsType, File baseDir,
      String serverType, boolean enableHDFSAcls) throws Exception {
    DFSType type;
    if(dfsType!=null) {
      type = DFSType.valueOf(dfsType.trim());
    }else {
      type = DFSType.MiniDFS;
    }
    switch (type) {
      case MiniDFS:
        return new MiniDFS(baseDir, serverType, enableHDFSAcls);
      case ClusterDFS:
        return new ClusterDFS();
      case S3DFS:
        return new S3DFS();
      default:
        throw new UnsupportedOperationException(type.name());
    }
  }

  public static DFS create(String dfsType, File baseDir,
                          String serverType) throws Exception {
    return create(dfsType, baseDir, serverType, false);
  }

  @VisibleForTesting
  public static enum DFSType {
    MiniDFS,
    ClusterDFS,
    S3DFS
  }
}
