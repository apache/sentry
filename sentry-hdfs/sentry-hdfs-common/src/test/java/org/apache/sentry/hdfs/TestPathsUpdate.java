/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sentry.hdfs;

import java.util.List;

import com.google.common.collect.Lists;
import org.apache.sentry.hdfs.service.thrift.TPathChanges;
import org.apache.sentry.hdfs.service.thrift.TPathsUpdate;
import org.apache.thrift.TException;
import org.junit.Test;
import junit.framework.Assert;

public class TestPathsUpdate {
  private List<String> uriToList(String uri) throws SentryMalformedPathException {
    String path = PathsUpdate.parsePath(uri);
    return Lists.newArrayList(path.split("/"));
  }

  @Test
  public void testParsePathComplexCharacters() throws SentryMalformedPathException{
    List<String> results = uriToList(
      "hdfs://hostname.test.com:8020/user/hive/warehouse/break/b=all | ' & the spaces/c=in PartKeys/With fun chars *%!|"
    );
    Assert.assertNotNull("Parse path without throwing exception",results);
  }

  @Test
  public void testPositiveParsePath() throws SentryMalformedPathException {
    String result = PathsUpdate.parsePath("hdfs://hostname.test.com:8020/path");
    Assert.assertTrue("Parsed path is unexpected", result.equals("path"));

    result = PathsUpdate.parsePath("hdfs://hostname.test.com/path");
    Assert.assertTrue("Parsed path is unexpected", result.equals("path"));

    result = PathsUpdate.parsePath("hdfs:///path");
    Assert.assertTrue("Parsed path is unexpected", result.equals("path"));
  }

  @Test(expected = SentryMalformedPathException.class)
  public void testMalformedPathFunny() throws SentryMalformedPathException{
    PathsUpdate.parsePath("hdfs://hostname");
  }

  //if file:// - should return null
  @Test
  public void testMalformedPathFile() throws SentryMalformedPathException{
    String result = PathsUpdate.parsePath("file://hostname/path");
    Assert.assertNull("Parse path without throwing exception",result);
  }

  @Test
  public void testSerializeDeserializeInJSON() throws SentryMalformedPathException, TException{
    PathsUpdate update = new PathsUpdate(1, true);
    TPathChanges pathChange = update.newPathChange("db1.tbl12");
    String path = PathsUpdate.parsePath("hdfs:///db1/tbl12/part121");
    List<String> paths = Lists.newArrayList(path.split("/"));
    pathChange.addToAddPaths(paths);

    // Serialize and deserialize the PermssionUpdate object should equals to the original one.
    TPathsUpdate before = update.toThrift();
    update.JSONDeserialize(update.JSONSerialize());
    junit.framework.Assert.assertEquals(before, update.toThrift());
  }
}
