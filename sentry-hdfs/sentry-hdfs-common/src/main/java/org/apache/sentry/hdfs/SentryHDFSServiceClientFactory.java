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

import java.lang.reflect.Proxy;

import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.hdfs.ha.HdfsHAClientInvocationHandler;
import org.apache.sentry.service.thrift.ServiceConstants.ServerConfig;

/**
 * Client factory to create normal client or proxy with HA invocation handler
 */
public class SentryHDFSServiceClientFactory {
  public static SentryHDFSServiceClient create(Configuration conf)
      throws Exception {
    boolean haEnabled = conf.getBoolean(ServerConfig.SENTRY_HA_ENABLED, false);
    if (haEnabled) {
      return (SentryHDFSServiceClient) Proxy.newProxyInstance(
          SentryHDFSServiceClientDefaultImpl.class.getClassLoader(),
          SentryHDFSServiceClientDefaultImpl.class.getInterfaces(),
          new HdfsHAClientInvocationHandler(conf));
    } else {
      return new SentryHDFSServiceClientDefaultImpl(conf);
    }
  }

}
