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

package org.apache.sentry.provider.db.service.persistent;

import org.apache.hadoop.conf.Configuration;

public class SentryStoreFactory {

  private static String STORE_TYPE_CONF = "sentry.store.type";
  private static String STORE_TYPE_DEF = "db";

  public static SentryStore createSentryStore(Configuration conf) {
    String storeType = conf.get(STORE_TYPE_CONF, STORE_TYPE_DEF);
    try {
      if ("db".equals(storeType)) {
        return new DbSentryStore(conf);
      } else if ("inmem".equals(storeType)) {
        return new SentryStoreWithLocalLock(new InMemSentryStore(conf));
      } else if ("localfile".equals(storeType)){
        return new SentryStoreWithLocalLock(
            new SentryStoreWithFileLog(new InMemSentryStore(conf)));
      } else {
        return new SentryStoreWithDistributedLock(
            new SentryStoreWithFileLog(
                new InMemSentryStore(conf)), HAContext.get(conf));
      }
    } catch (Exception e) {
      throw new RuntimeException("Could not instantiate Store !!", e);
    }
  }
}
