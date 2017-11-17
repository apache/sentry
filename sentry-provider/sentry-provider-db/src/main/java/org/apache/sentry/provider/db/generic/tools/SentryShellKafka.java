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

import com.google.common.base.Function;
import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.core.model.kafka.KafkaAuthorizable;
import org.apache.sentry.core.model.kafka.KafkaModelAuthorizables;
import org.apache.sentry.core.model.kafka.KafkaPrivilegeModel;
import org.apache.sentry.provider.common.AuthorizationComponent;

import javax.annotation.Nullable;

import static org.apache.sentry.core.common.utils.SentryConstants.*;

public class SentryShellKafka extends SentryShellGeneric {
  private static final String KAFKA_SERVICE_NAME = "sentry.service.client.kafka.service.name";

  @Override
  protected GenericPrivilegeConverter getPrivilegeConverter(String component, String service) {
    GenericPrivilegeConverter privilegeConverter = new GenericPrivilegeConverter(
            component,
            service,
            KafkaPrivilegeModel.getInstance().getPrivilegeValidators(),
            new KafkaModelAuthorizables(),
            true
    );
    privilegeConverter.setPrivilegeStrParser(new Function<String, String>() {
      @Nullable
      @Override
      public String apply(@Nullable String privilegeStr) {
        final String hostPrefix = KafkaAuthorizable.AuthorizableType.HOST.name() + KV_SEPARATOR;
        final String hostPrefixLowerCase = hostPrefix.toLowerCase();
        if (!privilegeStr.toLowerCase().startsWith(hostPrefixLowerCase)) {
          return hostPrefix + RESOURCE_WILDCARD_VALUE + AUTHORIZABLE_SEPARATOR + privilegeStr;
        }
        return privilegeStr;
      }
    });
    return privilegeConverter;
  }

  @Override
  protected String getComponent(Configuration conf) {
    return AuthorizationComponent.KAFKA;
  }

  @Override
  protected String getServiceName(Configuration conf) {
    return conf.get(KAFKA_SERVICE_NAME, AuthorizationComponent.KAFKA);
  }

  public static void main(String[] args) throws Exception {
    new SentryShellKafka().doMain(args);
  }
}
