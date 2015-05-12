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
package org.apache.sentry.sqoop;

import static junit.framework.Assert.fail;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.apache.sentry.provider.file.PolicyFiles;
import org.apache.sentry.sqoop.conf.SqoopAuthConf;
import org.apache.sentry.sqoop.conf.SqoopAuthConf.AuthzConfVars;
import org.apache.sqoop.security.SecurityFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.io.Files;
import com.google.common.io.Resources;

public class TestSentryAuthorizationHander {
  private static final String RESOURCE_PATH = "test-authz-provider.ini";
  private SqoopAuthConf authzConf;
  private File baseDir;

  @Before
  public void setup() throws Exception {
    baseDir = Files.createTempDir();
    PolicyFiles.copyToDir(baseDir, RESOURCE_PATH);
    authzConf = new SqoopAuthConf(Resources.getResource("sentry-site.xml"));
    authzConf.set(AuthzConfVars.AUTHZ_PROVIDER_RESOURCE.getVar(), new File(baseDir, RESOURCE_PATH).getPath());
  }

  @After
  public void teardown() {
    if(baseDir != null) {
      FileUtils.deleteQuietly(baseDir);
    }
  }

  /**
   * Test that incorrect specification of classes for
   * AUTHZ_ACCESS_CONTROLLER and AUTHZ_ACCESS_VALIDATOR
   * correctly throw ClassNotFoundExceptions
   */
  @Test
  public void testClassNotFound() throws Exception {
    try {
      SecurityFactory.getAuthorizationAccessController("org.apache.sentry.sqoop.authz.BogusSentryAccessController");
      fail("Exception should have been thrown");
    } catch (Exception ex) {
    }

    try {
      SecurityFactory.getAuthorizationValidator("org.apache.sentry.sqoop.authz.BogusSentryAuthorizationValidator");
      fail("Exception should have been thrown");
    } catch (Exception ex) {
    }
  }
}
