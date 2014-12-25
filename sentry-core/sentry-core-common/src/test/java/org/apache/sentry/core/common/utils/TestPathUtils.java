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
package org.apache.sentry.core.common.utils;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.net.URI;

import org.junit.Test;

public class TestPathUtils {

  @Test
  public void testNullScheme() throws Exception {
    assertTrue(PathUtils.impliesURI(new URI("/tmp"), new URI("/tmp/a")));
    assertFalse(PathUtils.impliesURI(new URI("file:/tmp"), new URI("/tmp/a")));
    assertFalse(PathUtils.impliesURI(new URI("/tmp"), new URI("file:/tmp/a")));
    // Privileges on /tmp/ are distinct from /tmp.+/ e.g. /tmp/ and /tmpdata/
    assertFalse(PathUtils.impliesURI(new URI("/tmp"), new URI("/tmpdata")));
  }

  @Test
  public void testParseDFSURI() throws Exception {
    // warehouse hdfs, path /
    assertEquals("hdfs://namenode:8020/tmp/hive-user", PathUtils.
      parseDFSURI("hdfs://namenode:8020/user/hive/warehouse", "/tmp/hive-user"));
    // warehouse hdfs, path hdfs
    assertEquals("hdfs://namenode:8020/tmp/hive-user", PathUtils.
      parseDFSURI("hdfs://namenode:8020/user/hive/warehouse", "hdfs://namenode:8020/tmp/hive-user"));
    try {
      PathUtils.parseDFSURI("hdfs://namenode:8020/user/hive/warehouse", "tmp/hive-user");
      fail("IllegalArgumentException should be thrown");
    } catch (IllegalArgumentException ue) {
    }

    // warehouse swift, path /
    assertEquals("swift://namenode:8020/tmp/hive-user",
        PathUtils.parseDFSURI("swift://namenode:8020/user/hive/warehouse", "/tmp/hive-user"));
    // warehouse swift, path swift
    assertEquals("swift://namenode:8020/tmp/hive-user", PathUtils.parseDFSURI(
        "swift://namenode:8020/user/hive/warehouse", "swift://namenode:8020/tmp/hive-user"));
    try {
      PathUtils.parseDFSURI("swift://namenode:8020/user/hive/warehouse", "tmp/hive-user");
      fail("IllegalArgumentException should be thrown");
    } catch (IllegalArgumentException ue) {
    }

    // warehouse file:///, path /
    assertEquals("file:///tmp/hive-user", PathUtils.
      parseDFSURI("file:///tmp/hive-warehouse", "/tmp/hive-user"));
    // warehouse file:///, path file:/
    assertEquals("file:///tmp/hive-user", PathUtils.
      parseDFSURI("file:///tmp/hive-warehouse", "file:/tmp/hive-user"));
    // warehouse file:///, path file:///
    assertEquals("file:///tmp/hive-user", PathUtils.
      parseDFSURI("file:///tmp/hive-warehouse", "file:///tmp/hive-user"));
    try {
      PathUtils.parseDFSURI("file:///hive-warehouse", "tmp/hive-user");
      fail("IllegalArgumentException should be thrown");
    } catch (IllegalArgumentException ue) {
    }

    // warehouse file:/, path /
    assertEquals("file:///tmp/hive-user", PathUtils.
      parseDFSURI("file:/tmp/hive-warehouse", "/tmp/hive-user"));
    // warehouse file:/, path file:/
    assertEquals("file:///tmp/hive-user", PathUtils.
      parseDFSURI("file:/tmp/hive-warehouse", "file:/tmp/hive-user"));
    // warehouse file:/, path file:///
    assertEquals("file:///tmp/hive-user", PathUtils.
      parseDFSURI("file:/tmp/hive-warehouse", "file:///tmp/hive-user"));

    // for local test case
    assertEquals("file:///tmp/hive-user",
        PathUtils.parseURI("testLocal:///tmp/hive-warehouse", "/tmp/hive-user", true));
    try {
      PathUtils.parseURI("testLocal:///tmp/hive-warehouse", "tmp/hive-user", true);
      fail("IllegalStateException should be thrown");
    } catch (IllegalArgumentException ue) {
    }

    // warehouse /, path /
    assertEquals("file:///tmp/hive-user",
        PathUtils.parseDFSURI("/tmp/hive-warehouse", "/tmp/hive-user"));
  }

  @Test
  public void testParseLocalURI() throws Exception {
    assertEquals("file:///tmp/hive-user", PathUtils.
      parseLocalURI("/tmp/hive-user"));
    assertEquals("file:///tmp/hive-user", PathUtils.
      parseLocalURI("file:/tmp/hive-user"));
    assertEquals("file:///tmp/hive-user", PathUtils.
      parseLocalURI("file:///tmp/hive-user"));
    assertEquals("file://localhost:9999/tmp/hive-user",
        PathUtils.parseLocalURI("file://localhost:9999/tmp/hive-user"));
  }
}
