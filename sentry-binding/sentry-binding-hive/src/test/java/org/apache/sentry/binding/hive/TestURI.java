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
package org.apache.sentry.binding.hive;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestURI {

  private static HiveConf conf;
  @BeforeClass
  public static void setupTestURI() {
    conf = new HiveConf();
    SessionState.start(conf);
  }

  @Test
  public void testParseURIIncorrectFilePrefix() throws SemanticException {
    Assert.assertEquals("file:///some/path",
        HiveAuthzBindingHook.parseURI("file:/some/path").getName());
  }
  @Test
  public void testParseURICorrectFilePrefix() throws SemanticException {
    Assert.assertEquals("file:///some/path",
        HiveAuthzBindingHook.parseURI("file:///some/path").getName());
  }
  @Test
  public void testParseURINoFilePrefix() throws SemanticException {
    conf.set(ConfVars.METASTOREWAREHOUSE.varname, "file:///path/to/warehouse");
    Assert.assertEquals("file:///some/path",
        HiveAuthzBindingHook.parseURI("/some/path").getName());
  }
  @Test
  public void testParseURINoHDFSPrefix() throws SemanticException {
    conf.set(ConfVars.METASTOREWAREHOUSE.varname, "hdfs://namenode:8080/path/to/warehouse");
    Assert.assertEquals("hdfs://namenode:8080/some/path",
        HiveAuthzBindingHook.parseURI("/some/path").getName());
  }
  @Test
  public void testParseURICorrectHDFSPrefix() throws SemanticException {
    Assert.assertEquals("hdfs:///some/path",
        HiveAuthzBindingHook.parseURI("hdfs:///some/path").getName());
  }
}