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
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.ParseUtils;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.sentry.core.model.db.AccessURI;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestHiveAuthzBindingHook {
  private static final Logger LOG = LoggerFactory
      .getLogger(TestHiveAuthzBindingHook.class);

  private ParseDriver parseDriver;
  private HiveConf conf;

  @Before
  public void setupTest() throws Exception {
    conf = new HiveConf();
    SessionState.start(conf);
    parseDriver = new ParseDriver();
  }

  @Test
  public void testAddPartition() throws Exception {
    ASTNode ast = parse("alter table parted add partition (day='Monday')");
    LOG.info("AST: " + ast.toStringTree());
    AccessURI partitionLocation = HiveAuthzBindingHook.extractPartition(ast);
    Assert.assertNull("Query without part location should not return location",
        partitionLocation);
  }
  @Test
  public void testAddPartitionWithLocation() throws Exception {
    ASTNode ast = parse("alter table parted add partition (day='Monday') location 'file:/'");
    LOG.info("AST: " + ast.toStringTree());
    AccessURI partitionLocation = HiveAuthzBindingHook.extractPartition(ast);
    Assert.assertNotNull("Query with part location must return location",
        partitionLocation);
    Assert.assertEquals("file:///", partitionLocation.getName());
  }

  @Test
  public void testAddPartitionIfNotExists() throws Exception {
    ASTNode ast = parse("alter table parted add if not exists partition (day='Monday')");
    LOG.info("AST: " + ast.toStringTree());
    AccessURI partitionLocation = HiveAuthzBindingHook.extractPartition(ast);
    Assert.assertNull("Query without part location should not return location",
        partitionLocation);
  }
  @Test
  public void testAddPartitionIfNotExistsWithLocation() throws Exception {
    ASTNode ast = parse("alter table parted add if not exists partition (day='Monday')" +
        " location 'file:/'");
    LOG.info("AST: " + ast.toStringTree());
    AccessURI partitionLocation = HiveAuthzBindingHook.extractPartition(ast);
    Assert.assertNotNull("Query with part location must return location",
        partitionLocation);
    Assert.assertEquals("file:///", partitionLocation.getName());
  }

  private ASTNode parse(String command) throws Exception {
    return ParseUtils.findRootNonNullToken(parseDriver.parse(command));
  }

}