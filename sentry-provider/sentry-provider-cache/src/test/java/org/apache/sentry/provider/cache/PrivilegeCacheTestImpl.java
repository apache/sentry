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

package org.apache.sentry.provider.cache;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.core.common.ActiveRoleSet;
import org.apache.sentry.provider.common.ProviderBackendContext;
import org.apache.sentry.provider.file.PolicyFiles;
import org.apache.sentry.provider.file.SimpleFileProviderBackend;

import com.google.common.io.Files;

/**
 * Test cache provider that is a wrapper on top of File based provider
 */
public class PrivilegeCacheTestImpl implements PrivilegeCache {
  private static final String resourcePath = "test-authz-provider-local-group-mapping.ini";

  private SimpleFileProviderBackend backend;
  private File baseDir;

  public PrivilegeCacheTestImpl() throws FileNotFoundException, IOException {
    baseDir = Files.createTempDir();
    PolicyFiles.copyToDir(baseDir, resourcePath);
    backend = new SimpleFileProviderBackend(new Configuration(), new File(baseDir, resourcePath)
      .toString());
    backend.initialize(new ProviderBackendContext());
  }

  @Override
  public Set<String> listPrivileges(Set<String> groups, ActiveRoleSet roleSet) {
    return backend.getPrivileges(groups, roleSet);
  }

  @Override
  public void close() {
    backend.close();
    if (baseDir != null) {
      FileUtils.deleteQuietly(baseDir);
    }
  }
}
