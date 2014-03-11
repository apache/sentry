/*
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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.sentry.provider.file;
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;

import org.apache.commons.io.FileUtils;
import org.apache.sentry.core.common.ActiveRoleSet;
import org.apache.sentry.provider.common.ProviderBackendContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Sets;
import com.google.common.io.Files;

public class TestSimpleFileProvderBackend {

  private static final String resourcePath = "test-authz-provider-local-group-mapping.ini";

  private SimpleFileProviderBackend backend;
  private ProviderBackendContext context;
  private File baseDir;

  @Before
  public void setup() throws IOException {
    baseDir = Files.createTempDir();
    PolicyFiles.copyToDir(baseDir, resourcePath);
    backend = new SimpleFileProviderBackend(new File(baseDir, resourcePath).toString());
    context = new ProviderBackendContext();
  }

  @After
  public void teardown() {
    if(baseDir != null) {
      FileUtils.deleteQuietly(baseDir);
    }
  }

  @Test
  public void testInitializeTwice() {
    backend.initialize(context);
    try {
      backend.initialize(context);
      fail("Expected IllegalStateException on second initialze");
    } catch (IllegalStateException e) {
      // expected
    }
  }

  @Test(expected = IllegalStateException.class)
  public void testUninitializeGetPrivileges() {
    backend.getPrivileges(new HashSet<String>(), ActiveRoleSet.ALL);
  }

  @Test(expected = IllegalStateException.class)
  public void testUninitializeValidatePolicy() {
    backend.validatePolicy(true);
  }

  @Test
  public void testRoleSetAll() {
    backend.initialize(context);
    assertEquals(Sets.newHashSet("server=server1->db=customers->table=purchases->select",
        "server=server1->db=analyst1", "server=server1->db=jranalyst1->table=*->select",
        "server=server1->db=jranalyst1", "server=server1->functions"),
        backend.getPrivileges(Sets.newHashSet("manager"), ActiveRoleSet.ALL));
  }

  @Test
  public void testRoleSetAllUnknownGroup() {
    backend.initialize(context);
    assertEquals(Sets.newHashSet(), backend.getPrivileges(Sets.newHashSet("not-a-group"),
        ActiveRoleSet.ALL));
  }

  @Test
  public void testRoleSetNone() {
    backend.initialize(context);
    assertEquals(Sets.newHashSet(), backend.getPrivileges(Sets.newHashSet("manager"),
        new ActiveRoleSet(new HashSet<String>())));
  }

  @Test
  public void testRoleSetOne() {
    backend.initialize(context);
    assertEquals(Sets.newHashSet("server=server1->functions"),
        backend.getPrivileges(Sets.newHashSet("manager"),
            new ActiveRoleSet(Sets.newHashSet("functions"))));
  }

  @Test
  public void testRoleSetTwo() {
    backend.initialize(context);
    assertEquals(Sets.newHashSet("server=server1->db=jranalyst1",
        "server=server1->functions"),
        backend.getPrivileges(Sets.newHashSet("manager"),
            new ActiveRoleSet(Sets.newHashSet("junior_analyst_role", "functions"))));
  }
}
