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
package org.apache.sentry.sqoop.binding;

import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.core.model.sqoop.Server;
import org.apache.sentry.provider.common.AuthorizationComponent;
import org.apache.sentry.provider.db.generic.SentryGenericProviderBackend;
import org.apache.sentry.sqoop.conf.SqoopAuthConf.AuthzConfVars;

public class SqoopProviderBackend extends SentryGenericProviderBackend {
  private Server sqoopServer;
  public SqoopProviderBackend(Configuration conf, String resourcePath) throws Exception {
    super(conf);
    sqoopServer = new Server(conf.get(AuthzConfVars.AUTHZ_SERVER_NAME.getVar()));
  }
  @Override
  public String getComponentType() {
    return AuthorizationComponent.SQOOP;
  }

  /**
   * SqoopProviderBackend use the name of Sqoop Server as the identifier to
   * distinguish itself from multiple Sqoop Servers
   */
  @Override
  public String getComponentIdentifier() {
    return sqoopServer.getName();
  }
}
