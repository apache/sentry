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
package org.apache.sentry.policy.kafka;

import static org.apache.sentry.provider.common.ProviderConstants.AUTHORIZABLE_SPLITTER;

import java.util.List;

import org.apache.sentry.core.model.kafka.KafkaActionConstant;
import org.apache.sentry.core.model.kafka.KafkaAuthorizable;
import org.apache.sentry.policy.common.Privilege;
import org.apache.sentry.policy.common.PrivilegeFactory;
import org.apache.sentry.provider.common.KeyValue;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

public class KafkaWildcardPrivilege implements Privilege {

  private static String ALL_HOSTS = "*";

  public static class Factory implements PrivilegeFactory {
    @Override
    public Privilege createPrivilege(String permission) {
      return new KafkaWildcardPrivilege(permission);
    }
  }

  private final ImmutableList<KeyValue> parts;

  public KafkaWildcardPrivilege(String permission) {
    if (Strings.isNullOrEmpty(permission)) {
      throw new IllegalArgumentException("Permission string cannot be null or empty.");
    }
    List<KeyValue>parts = Lists.newArrayList();
    for (String authorizable : AUTHORIZABLE_SPLITTER.trimResults().split(permission.trim())) {
      if (authorizable.isEmpty()) {
        throw new IllegalArgumentException("Privilege '" + permission + "' has an empty section");
      }
      parts.add(new KeyValue(authorizable));
    }
    if (parts.isEmpty()) {
      throw new AssertionError("Privilege,  " + permission + ", did not consist of any valid authorizable.");
    }
    this.parts = ImmutableList.copyOf(parts);
  }

  @Override
  public boolean implies(Privilege p) {
    if (!(p instanceof KafkaWildcardPrivilege)) {
      return false;
    }
    KafkaWildcardPrivilege wp = (KafkaWildcardPrivilege)p;
    List<KeyValue> otherParts = wp.parts;
    if(equals(wp)) {
      return true;
    }
    int index = 0;
    for (KeyValue otherPart : otherParts) {
      // If this privilege has less parts than the other privilege, everything
      // after the number of parts contained
      // in this privilege is automatically implied, so return true
      if (parts.size() - 1 < index) {
        return true;
      } else {
        KeyValue part = parts.get(index);
        // Support for action inheritance from parent to child
        if (part.getKey().equalsIgnoreCase(KafkaActionConstant.actionName)
            && !(otherPart.getKey().equalsIgnoreCase(KafkaActionConstant.actionName))) {
          continue;
        }
        // are the keys even equal
        if(!part.getKey().equalsIgnoreCase(otherPart.getKey())) {
          return false;
        }
        if (!impliesKeyValue(part, otherPart)) {
          return false;
        }
        index++;
      }
    }
    // If this privilege has more parts than
    // the other parts, only imply it if
    // all of the other parts are "*" or "ALL"
    for (; index < parts.size(); index++) {
      KeyValue part = parts.get(index);
      if (!part.getValue().equals(KafkaActionConstant.ALL)) {
        return false;
      }
    }
    return true;
  }

  private boolean impliesKeyValue(KeyValue policyPart, KeyValue requestPart) {
    Preconditions.checkState(policyPart.getKey().equalsIgnoreCase(requestPart.getKey()),
        "Please report, this method should not be called with two different keys");

    // Host is a special resource, not declared as resource in Kafka. Each Kafka resource can be
    // authorized based on the host request originated from and to handle this, Sentry uses host as
    // a resource. Kafka allows using '*' as wildcard for all hosts. '*' however is not a valid
    // Kafka action.
    if (hasHostWidCard(policyPart)) {
      return true;
    }

    if (KafkaActionConstant.actionName.equalsIgnoreCase(policyPart.getKey())) { // is action
      return policyPart.getValue().equalsIgnoreCase(KafkaActionConstant.ALL) ||
          policyPart.equals(requestPart);
    } else {
      return policyPart.getValue().equals(requestPart.getValue());
    }
  }

  private boolean hasHostWidCard(KeyValue policyPart) {
    if (policyPart.getKey().equalsIgnoreCase(KafkaAuthorizable.AuthorizableType.HOST.toString()) &&
        policyPart.getValue().equalsIgnoreCase(ALL_HOSTS)) {
      return true;
    }
    return false;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    for(KeyValue kv: this.parts) {
      sb.append(kv.getKey() + "=" + kv.getValue() + "->");
    }
    return sb.toString();
  }
}
