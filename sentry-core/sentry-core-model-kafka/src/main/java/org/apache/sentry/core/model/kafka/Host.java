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
 * Represents Host authorizable in Kafka model
 */
public class Host implements KafkaAuthorizable {
  private String name;

  /**
   * Create a Kafka's Host authorizable of a given string representation.
   *
   * @param name String representation of host.
   */
  public Host(String name) {
    this.name = name;
  }

  /**
   * Get authorizable type of Host authorizable.
   *
   * @return Type of Host authorizable.
   */
  @Override
  public AuthorizableType getAuthzType() {
    return AuthorizableType.HOST;
  }

  /**
   * Get name of Kafka's host authorizable.
   *
   * @return Name of Kafka's host authorizable.
   */
  @Override
  public String getName() {
    return name;
  }

  /**
   * Get type name of Kafka's host authorizable.
   *
   * @return Type name of Kafka's host authorizable.
   */
  @Override
  public String getTypeName() {
    return getAuthzType().name();
  }

}
