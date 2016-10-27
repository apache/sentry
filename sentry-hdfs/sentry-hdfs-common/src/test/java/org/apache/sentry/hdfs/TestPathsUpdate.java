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

import org.junit.Test;
import junit.framework.Assert;

public class TestPathsUpdate {
  @Test
  public void testParsePathComplexCharacters() throws SentryMalformedPathException{
    List<String> results = PathsUpdate.parsePath(
      "hdfs://hostname.test.com:8020/user/hive/warehouse/break/b=all | ' & the spaces/c=in PartKeys/With fun chars *%!|"
    );
    System.out.println(results);
    Assert.assertNotNull("Parse path without throwing exception",results);
  }

  @Test
  public void testPositiveParsePath() throws SentryMalformedPathException {
    List<String> results = PathsUpdate.parsePath("hdfs://hostname.test.com:8020/path");
    Assert.assertTrue("Parsed path is unexpected", results.get(0).equals("path"));
    Assert.assertTrue("Parsed path size is unexpected", results.size() == 1);

    results = PathsUpdate.parsePath("hdfs://hostname.test.com/path");
    Assert.assertTrue("Parsed path is unexpected", results.get(0).equals("path"));
    Assert.assertTrue("Parsed path size is unexpected", results.size() == 1);

    results = PathsUpdate.parsePath("hdfs:///path");
    Assert.assertTrue("Parsed path is unexpected", results.get(0).equals("path"));
    Assert.assertTrue("Parsed path size is unexpected", results.size() == 1);
  }

  @Test(expected = SentryMalformedPathException.class)
  public void testMalformedPathFunny() throws SentryMalformedPathException{
    PathsUpdate.parsePath("hdfs://hostname");
  }

  //if file:// - should return null
  @Test
  public void testMalformedPathFile() throws SentryMalformedPathException{
    List<String> results = PathsUpdate.parsePath("file://hostname/path");
    System.out.println(results);
    Assert.assertNull("Parse path without throwing exception",results);
  }
}
