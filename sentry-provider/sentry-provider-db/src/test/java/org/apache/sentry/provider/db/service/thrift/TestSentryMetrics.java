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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sentry.provider.db.service.thrift;

import com.codahale.metrics.Counter;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.service.thrift.ServiceConstants;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;

import static java.lang.Thread.sleep;

public class TestSentryMetrics {
  private static SentryMetrics metrics = SentryMetrics.getInstance();
  private final static Configuration conf = new Configuration();
  private static File jsonReportFile;

  @BeforeClass
  public static void setUp() throws Exception {
    jsonReportFile = File.createTempFile("TestMetrics", ".json");
    String jsonFile = jsonReportFile.getAbsolutePath();
    conf.set(ServiceConstants.ServerConfig.SENTRY_JSON_REPORTER_FILE, jsonFile);
    conf.setInt(ServiceConstants.ServerConfig.SENTRY_REPORTER_INTERVAL_SEC, 1);
    conf.set(ServiceConstants.ServerConfig.SENTRY_REPORTER, "JSON");
    metrics.initReporting(conf);
  }

  @AfterClass
  public static void cleanup() {
    System.out.println(jsonReportFile);
    jsonReportFile.delete();
  }


  /**
   * Test JSON reporter.
   * <ul>
   *   <li>increment the counter value</li>
   *   <li>wait a bit for the new repor to be written</li>
   *   <li>read the value from JSON file</li>
   *   <li>verify that the value matches expectation</li>
   * </ul>
   * This check is repeated a few times to verify that the values are updated over time.
   * @throws Exception if fails to read counter value
   */
  @Test
  public void testJsonReporter() throws Exception {
    int runs = 5;
    String  counterName = "cnt";
    Counter counter = metrics.getCounter(counterName);
    for (int i = 0; i < runs; i++) {
      counter.inc();
      sleep(1500);
      Assert.assertEquals(i + 1, getCounterValue(counterName));
    }

  }

  /**
   * Read counter value from JSON metric report
   * @param name counter name
   * @return counter value
   * @throws FileNotFoundException if file doesn't exist
   */
  private int getCounterValue(String name) throws FileNotFoundException {
    JsonParser parser = new JsonParser();
    JsonElement element = parser.parse(new FileReader(jsonReportFile.getAbsolutePath()));
    JsonObject jobj = element.getAsJsonObject();
    jobj = jobj.getAsJsonObject("counters").getAsJsonObject(name);
    return jobj.get("count").getAsInt();
  }
}