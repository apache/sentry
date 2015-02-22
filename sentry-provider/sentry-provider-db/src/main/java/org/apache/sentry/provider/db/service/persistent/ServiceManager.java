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

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.ServiceProvider;
import org.apache.curator.x.discovery.details.InstanceSerializer;
import org.apache.hadoop.net.NetUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/***
 * ServerManager handles registration of the Sentry service for Curator service
 * discovery. Each server registers with ZK and add its host:port details which
 * is used by the clients to discover available servers
 */
public class ServiceManager {
  private static final Logger LOGGER = LoggerFactory
      .getLogger(ServiceManager.class);
  private HAContext haContext;
  private ServiceProvider<Void> serviceProvider;
  private ServiceDiscovery<Void> serviceDiscovery;

  public ServiceManager(HAContext haContext) throws IOException {
    this.haContext = haContext;
    init();
  }

  private void init() throws IOException {
    try {
      haContext.startCuratorFramework();
      InstanceSerializer<Void> instanceSerializer = new FixedJsonInstanceSerializer<Void>(Void.class);
      serviceDiscovery = ServiceDiscoveryBuilder.<Void>builder(Void.class)
                .basePath(HAContext.SENTRY_SERVICE_REGISTER_NAMESPACE)
                .serializer(instanceSerializer)
          .client(haContext.getCuratorFramework())
                .build();
      serviceDiscovery.start();
      serviceProvider = serviceDiscovery
              .serviceProviderBuilder()
              .serviceName(HAContext.SENTRY_SERVICE_REGISTER_NAMESPACE)
              .build();
      serviceProvider.start();
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  public ServiceInstance<Void> getServiceInstance() throws IOException {
    ServiceInstance<Void> service;
    try {
      service = serviceProvider.getInstance();
      return service;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  public void reportError(ServiceInstance<Void> instance) {
    serviceProvider.noteError(instance);
  }

  public static InetSocketAddress convertServiceInstance(ServiceInstance<?> service) {
    return NetUtils.createSocketAddr(service.getAddress(),service.getPort());
  }

  public void close() {
    try {
      serviceProvider.close();
      serviceDiscovery.close();
      LOGGER.debug("Closed ZK resources");
    } catch (IOException e) {
      LOGGER.warn("Error closing the service manager", e);
    }
  }
}
