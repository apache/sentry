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

package org.apache.sentry.provider.file;

import java.io.File;
import java.io.IOException;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Sets;
import com.google.common.io.Files;

public class TestLocalGroupMapping {

  private static final String resourcePath = "test-authz-provider-local-group-mapping.ini";
  private static final Set<String> fooGroups = Sets.newHashSet("admin", "analyst");
  private static final Set<String> barGroups = Sets.newHashSet("jranalyst");

  private LocalGroupMappingService localGroupMapping;

  private File baseDir;

  @Before
  public void setup() throws IOException {
    baseDir = Files.createTempDir();
    PolicyFiles.copyToDir(baseDir, resourcePath);
    localGroupMapping = new LocalGroupMappingService(new Path(new File(baseDir, resourcePath).getPath()));
  }

  @After
  public void teardown() {
    if(baseDir != null) {
      FileUtils.deleteQuietly(baseDir);
    }
  }

  @Test
  public void testGroupMapping() {
    Set<String> fooGroupsFromResource = localGroupMapping.getGroups("foo");
    Assert.assertEquals(fooGroupsFromResource, fooGroups);

    Set<String> barGroupsFromResource = localGroupMapping.getGroups("bar");
    Assert.assertEquals(barGroupsFromResource, barGroups);

    Set<String> unknownGroupsFromResource = localGroupMapping.getGroups("unknown");
    Assert.assertTrue("List not empty " + unknownGroupsFromResource, unknownGroupsFromResource.isEmpty());
  }
}
