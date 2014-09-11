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

package org.apache.sentry.provider.db.service.thrift;

import java.io.File;
import java.util.Properties;

import org.apache.hadoop.minikdc.MiniKdc;
import org.junit.After;

public class SentryMiniKdcTestcase {

  private File workDir;
  private Properties conf;
  private MiniKdc kdc;

  public void startMiniKdc(Properties confOverlay) throws Exception {
    createTestDir();
    createMiniKdcConf(confOverlay);
    kdc = new MiniKdc(conf, workDir);
    kdc.start();
  }

  private void createMiniKdcConf(Properties confOverlay) {
    conf = MiniKdc.createConf();
    for ( Object property : confOverlay.keySet()) {
      conf.put(property, confOverlay.get(property));
    }
  }

  private void createTestDir() {
    workDir = new File(System.getProperty("test.dir", "target"));
  }

  @After
  public void stopMiniKdc() {
    if (kdc != null) {
      kdc.stop();
    }
  }

  public MiniKdc getKdc() {
    return kdc;
  }

  public File getWorkDir() {
    return workDir;
  }

  public Properties getConf() {
    return conf;
  }

}
