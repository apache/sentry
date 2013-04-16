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

package org.apache.access.provider.file;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.io.Files;

public class TestSimplePollingPolicy {

  private SimplePolicy policy;
  private SimplePollingPolicy pollingPolicy;
  private File baseDir;
  private File resourceFile;

  @Before
  public void setup() throws IOException {
    policy = mock(SimplePolicy.class);
    baseDir = Files.createTempDir();
    resourceFile = new File(baseDir, "policy.ini");
    Assert.assertTrue(resourceFile.createNewFile());
  }

  @After
  public void teardown() {
    FileUtils.deleteQuietly(baseDir);
    if(pollingPolicy != null) {
      pollingPolicy.shutdown();
    }
  }

  @Test
  public void testPolling() throws Exception {
    when(policy.getModificationTime()).thenReturn(1L);
    pollingPolicy = new SimplePollingPolicy(policy, 1);
    when(policy.getModificationTime()).thenReturn(System.currentTimeMillis());
    TimeUnit.SECONDS.sleep(3);
    verify(policy).parse();
  }
}
