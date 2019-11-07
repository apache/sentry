/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sentry.kafka;

import java.util.List;

import com.google.common.collect.Lists;
import kafka.security.auth.Resource;
import kafka.security.auth.ResourceType$;
import org.apache.sentry.api.generic.thrift.TAuthorizable;
import org.apache.sentry.core.common.Authorizable;
import org.apache.sentry.core.model.kafka.KafkaAuthorizable;
import org.apache.sentry.core.model.kafka.Host;

public class ConvertUtil {

  public static List<Authorizable> convertResourceToAuthorizable(String hostname,
      final Resource resource) {
    List<Authorizable> authorizables = Lists.newArrayList();
    authorizables.add(new Host(hostname));
    authorizables.add(new Authorizable() {
      @Override
      public String getTypeName() {
        final String resourceTypeName = resource.resourceType().name();
        // Kafka's GROUP resource is referred as CONSUMERGROUP within Sentry.
        if (resourceTypeName.equalsIgnoreCase("group")) {
          return KafkaAuthorizable.AuthorizableType.CONSUMERGROUP.name();
        } else {
          return resourceTypeName;
        }
      }

      @Override
      public String getName() {
        return resource.name();
      }
    });
    return authorizables;
  }

  public static Resource convertAuthorizableToResource(TAuthorizable tAuthorizable) {
    // Kafka's GROUP resource is referred as CONSUMERGROUP within Sentry.
    String authorizableType = tAuthorizable.getType().equalsIgnoreCase("consumergroup") ? "group" : tAuthorizable.getType();
    Resource resource = new Resource(ResourceType$.MODULE$.fromString(authorizableType), tAuthorizable.getName());

    return resource;
  }
}
