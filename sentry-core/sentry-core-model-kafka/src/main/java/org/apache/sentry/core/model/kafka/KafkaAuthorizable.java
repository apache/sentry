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
package org.apache.sentry.core.model.kafka;

import org.apache.sentry.core.common.Authorizable;

/**
 * This interface represents authorizable resource in Kafka component.
 * It uses conjunction with generic authorization model (SENTRY-398).
 *
 * Authorizables here are mapped to Kafka resources based on below mentioned mapping.
 *
 * CLUSTER ->       Kafka Cluster resource, users are required to have access to this resource in
 *                  order to perform cluster level actions like create topic, delete topic, etc.
 *
 * HOST ->          Kafka allows to authorize requests based on the host it is coming from. Though,
 *                  Host is not a resource in Kafka, each Kafka Acl has host in it. In order to
 *                  provide host based resource authorization, Host is treated as a Kafka resource
 *                  in Sentry.
 *
 * TOPIC ->         Kafka Topic resource, users are required to have access to this resource in
 *                  order to perform topic level actions like reading from a topic, writing to a
 *                  topic, etc.
 *
 * CONSUMERGROUP -> Kafka ConsumerGroup resource, users are required to have access to this resource
 *                  in order to perform ConsumerGroup level actions like joining a consumer group,
 *                  querying offset for a partition for a particular consumer group.
 *
 *  TRANSACTIONALID -> This resource represents actions related to transactions, such as committing.
 *
 */
public interface KafkaAuthorizable extends Authorizable {
  /**
   * Types of resources that Kafka supports authorization on.
   */
  public enum AuthorizableType {
    CLUSTER,
    HOST,
    TOPIC,
    CONSUMERGROUP,
    TRANSACTIONALID
  };

  /**
   * Get type of this Kafka authorizable.
   * @return Type of this Kafka authorizable.
   */
  AuthorizableType getAuthzType();
}
