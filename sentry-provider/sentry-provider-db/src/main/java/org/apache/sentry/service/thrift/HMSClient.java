/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.sentry.service.thrift;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;

/**
 * AutoCloseable wrapper around HiveMetaStoreClient.
 * It is only used to provide try-with-resource semantics for
 * {@link HiveMetaStoreClient}.
 */
class HMSClient implements AutoCloseable {
  private final HiveMetaStoreClient client;
  private boolean valid;

  HMSClient(HiveMetaStoreClient client) {
    this.client = Preconditions.checkNotNull(client);
    valid = true;
  }

  public HiveMetaStoreClient getClient() {
    return client;
  }

  public void invalidate() {
    if (valid) {
      client.close();
      valid = false;
    }
  }

  @Override
  public void close() {
    if (valid) {
      client.close();
      valid = false;
    }
  }
}
