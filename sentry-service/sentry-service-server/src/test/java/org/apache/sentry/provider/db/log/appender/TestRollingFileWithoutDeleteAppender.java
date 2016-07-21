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

package org.apache.sentry.provider.db.log.appender;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.io.Files;

public class TestRollingFileWithoutDeleteAppender {
  private Logger sentryLogger = Logger.getRootLogger();
  private File dataDir;

  @Before
  public void init() {
    dataDir = Files.createTempDir();
  }

  @Test
  public void testRollOver() throws Throwable {
    if (dataDir == null) {
      Assert.fail("Excepted temp folder for audit log is created.");
    }
    RollingFileWithoutDeleteAppender appender = new RollingFileWithoutDeleteAppender(
        new PatternLayout("%m%n"), dataDir.getPath() + "/auditLog.log");
    appender.setMaximumFileSize(100);
    sentryLogger.addAppender(appender);
    // Write exactly 10 bytes with each log
    for (int i = 0; i < 99; i++) {
      if (i < 10) {
        sentryLogger.debug("Hello---" + i);
      } else if (i < 100) {
        sentryLogger.debug("Hello--" + i);
      }
    }

    if (dataDir != null) {
      File[] files = dataDir.listFiles();
      if (files != null) {
        Assert.assertEquals(files.length, 10);
      } else {
        Assert.fail("Excepted 10 log files.");
      }
    } else {
      Assert.fail("Excepted 10 log files.");
    }

  }

  /***
   * Generate log enough to cause a single rollover. Verify the file name format
   * @throws Throwable
   */
  @Test
  public void testFileNamePattern() throws Throwable {
    if (dataDir == null) {
      Assert.fail("Excepted temp folder for audit log is created.");
    }
    RollingFileWithoutDeleteAppender appender = new RollingFileWithoutDeleteAppender(
        new PatternLayout("%m%n"), dataDir.getPath() + "/auditLog.log");
    appender.setMaximumFileSize(10);
    sentryLogger.addAppender(appender);
    sentryLogger.debug("123456789012345");
    File[] files = dataDir.listFiles();
    if (files != null) {
      Assert.assertEquals(files.length, 2);
      Assert.assertTrue(files[0].getName().contains("auditLog.log."));
      Assert.assertTrue(files[1].getName().contains("auditLog.log."));
    } else {
      Assert.fail("Excepted 2 log files.");
    }
  }

  @After
  public void destroy() {
    if (dataDir != null) {
      FileUtils.deleteQuietly(dataDir);
    }
  }
}
