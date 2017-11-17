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
package org.apache.sentry.provider.db.generic.tools;

import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.core.model.sqoop.SqoopModelAuthorizables;
import org.apache.sentry.core.model.sqoop.SqoopPrivilegeModel;
import org.apache.sentry.provider.common.AuthorizationComponent;

public class SentryShellSqoop extends SentryShellGeneric {
  private static final String SQOOP_SERVICE_NAME = "sentry.service.client.sqoop.service.name";

  @Override
  protected GenericPrivilegeConverter getPrivilegeConverter(String component, String service) {
    return new GenericPrivilegeConverter(
            component,
            service,
            SqoopPrivilegeModel.getInstance().getPrivilegeValidators(service),
            new SqoopModelAuthorizables(),
            true
    );
  }

  @Override
  protected String getComponent(Configuration conf) {
    return AuthorizationComponent.SQOOP;
  }

  @Override
  protected String getServiceName(Configuration conf) {
    return conf.get(SQOOP_SERVICE_NAME, "sqoopServer1");
  }

  public static void main(String[] args) throws Exception {
    new SentryShellSqoop().doMain(args);
  }
}
