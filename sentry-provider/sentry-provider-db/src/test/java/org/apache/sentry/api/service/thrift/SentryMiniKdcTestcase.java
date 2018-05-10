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

package org.apache.sentry.api.service.thrift;

import java.io.File;
import java.util.Properties;

import org.apache.hadoop.minikdc.MiniKdc;

public class SentryMiniKdcTestcase {

  private static File workDir;
  private static Properties conf;
  private static MiniKdc kdc;

  public static void startMiniKdc(Properties confOverlay) throws Exception {
    createTestDir();
    createMiniKdcConf(confOverlay);
    kdc = new MiniKdc(conf, workDir);
    kdc.start();
  }

  private static void createMiniKdcConf(Properties confOverlay) {
    conf = MiniKdc.createConf();
    for ( Object property : confOverlay.keySet()) {
      conf.put(property, confOverlay.get(property));
    }
  }

  private static void createTestDir() {
    workDir = new File(System.getProperty("test.dir", "target"));
  }

  public static void stopMiniKdc() {
    if (kdc != null) {
      kdc.stop();
    }
  }

  public static MiniKdc getKdc() {
    return kdc;
  }

  public static File getWorkDir() {
    return workDir;
  }

  public Properties getConf() {
    return conf;
  }

}
