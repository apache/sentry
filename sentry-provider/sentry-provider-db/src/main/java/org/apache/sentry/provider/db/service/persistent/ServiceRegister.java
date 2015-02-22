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

import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.details.InstanceSerializer;

public class ServiceRegister {

  private HAContext haContext;

  public ServiceRegister(HAContext haContext) {
    this.haContext = haContext;
  }

  public void regService(String host, int port) throws Exception {

    haContext.startCuratorFramework();
    ServiceInstance<Void> serviceInstance = ServiceInstance.<Void>builder()
        .address(host)
        .port(port)
        .name(HAContext.SENTRY_SERVICE_REGISTER_NAMESPACE)
        .build();

    InstanceSerializer<Void> instanceSerializer = new FixedJsonInstanceSerializer<Void>(Void.class);
    ServiceDiscoveryBuilder.builder(Void.class)
        .basePath(HAContext.SENTRY_SERVICE_REGISTER_NAMESPACE)
        .client(haContext.getCuratorFramework())
        .serializer(instanceSerializer)
        .thisInstance(serviceInstance)
        .build()
        .start();
  }

}
