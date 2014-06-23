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

import java.net.URI;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.fest.reflect.core.Reflection;

public class InternalMetastoreServer extends AbstractHiveServer {
  private final HiveConf conf;
  private ExecutorService metaStoreExecutor = Executors
      .newSingleThreadExecutor();

  public InternalMetastoreServer(HiveConf conf) throws Exception {
    super(conf, getMetastoreHostname(conf), getMetastorePort(conf));
    // Fix for ACCESS-148. Resets a static field
    // so the default database is created even
    // though is has been created before in this JVM
    Reflection.staticField("createDefaultDB").ofType(boolean.class)
        .in(HiveMetaStore.HMSHandler.class).set(false);
    this.conf = conf;
  }

  @Override
  public String getURL() {
    return "jdbc:hive2://";
  }

  @Override
  public void start() throws Exception {
    startMetastore();
  }

  @Override
  public void shutdown() throws Exception {
    metaStoreExecutor.shutdown();
  }

  // async metastore startup since Hive doesn't have that option
  private void startMetastore() throws Exception {
    Callable<Void> metastoreService = new Callable<Void>() {
      public Void call() throws Exception {
        try {
          HiveMetaStore.startMetaStore(getMetastorePort(conf),
              ShimLoader.getHadoopThriftAuthBridge(), conf);
        } catch (Throwable e) {
          throw new Exception("Error starting metastore", e);
        }
        return null;
      }
    };
    metaStoreExecutor.submit(metastoreService);
  }

  private static String getMetastoreHostname(Configuration conf)
      throws Exception {
    return new URI(conf.get(HiveConf.ConfVars.METASTOREURIS.varname)).getHost();
  }

  private static int getMetastorePort(Configuration conf) throws Exception {
    return new URI(conf.get(HiveConf.ConfVars.METASTOREURIS.varname)).getPort();

  }
}
