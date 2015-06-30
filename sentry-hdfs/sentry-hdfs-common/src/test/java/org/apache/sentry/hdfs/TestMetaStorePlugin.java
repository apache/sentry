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
package org.apache.sentry.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.Assert;
import org.junit.Test;
import org.apache.sentry.hdfs.ServiceConstants.ClientConfig;

public class TestMetaStorePlugin {

  @Test
  public void testPropertiesBackwardCompatibility() {
    Configuration hiveConf = new HiveConf();
    Configuration sentryConf = new Configuration();
    String[] properties = {ClientConfig.SERVER_RPC_ADDRESS, ClientConfig.SERVER_RPC_PORT,
      ClientConfig.SERVER_RPC_CONN_TIMEOUT, ClientConfig.SECURITY_MODE, ClientConfig.SECURITY_USE_UGI_TRANSPORT,
      ClientConfig.PRINCIPAL, ClientConfig.USE_COMPACT_TRANSPORT};
    for (String property: properties) {
      hiveConf.set(property, "blah");
    }
    MetastorePlugin metastorePlugin = new MetastorePlugin(hiveConf, sentryConf);
    sentryConf = metastorePlugin.getSentryConf();
    for (String property: properties) {
      Assert.assertEquals("Following property cannot be read from sentry-site: " + property, "blah",
          sentryConf.get(property));
    }
  }
}
