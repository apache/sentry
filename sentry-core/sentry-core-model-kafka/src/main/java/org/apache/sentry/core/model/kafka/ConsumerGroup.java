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
/**
 * Represents the ConsumerGroup authorizable in the Kafka model
 */
public class ConsumerGroup implements KafkaAuthorizable {
  private String name;

  /**
   * Create a Consumer-Group authorizable for Kafka cluster of a given name.
   *
   * @param name Name of Consumer-Group in a Kafka cluster.
   */
  public ConsumerGroup(String name) {
    this.name = name;
  }

  /**
   * Get type of Kafka's consumer-group authorizable.
   *
   * @return Type of Kafka's consumer-group authorizable.
   */
  @Override
  public AuthorizableType getAuthzType() {
    return AuthorizableType.CONSUMERGROUP;
  }

  /**
   * Get name of Kafka's consumer-group.
   *
   * @return Name of Kafka's consumer-group.
   */
  @Override
  public String getName() {
    return name;
  }

  /**
   * Get type name of Kafka's consumer-group authorizable.
   *
   * @return Type name of Kafka's consumer-group authorizable.
   */
  @Override
  public String getTypeName() {
    return getAuthzType().name();
  }
}
