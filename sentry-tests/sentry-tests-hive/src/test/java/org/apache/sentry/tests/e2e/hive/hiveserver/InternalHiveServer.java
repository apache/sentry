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

package org.apache.sentry.tests.e2e.hive.hiveserver;

import java.io.IOException;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hive.service.server.HiveServer2;
import org.fest.reflect.core.Reflection;

public class InternalHiveServer extends AbstractHiveServer {

  private final HiveServer2 hiveServer2;
  private final HiveConf conf;

  public InternalHiveServer(HiveConf conf) throws IOException {
    super(conf, getHostname(conf), getPort(conf));
    // Fix for ACCESS-148. Resets a static field
    // so the default database is created even
    // though is has been created before in this JVM
    Reflection.staticField("createDefaultDB")
      .ofType(boolean.class)
      .in(HiveMetaStore.HMSHandler.class)
      .set(false);
    hiveServer2 = new HiveServer2();
    this.conf = conf;
  }

  @Override
  public synchronized void start() throws Exception {
    hiveServer2.init(conf);
    hiveServer2.start();
    waitForStartup(this);
  }

  @Override
  public synchronized void shutdown() {
    hiveServer2.stop();
  }
}