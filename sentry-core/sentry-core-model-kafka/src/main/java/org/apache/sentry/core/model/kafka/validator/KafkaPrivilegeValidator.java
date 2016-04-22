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
package org.apache.sentry.core.model.kafka.validator;

import static org.apache.sentry.core.common.utils.SentryConstants.AUTHORIZABLE_SPLITTER;
import static org.apache.sentry.core.common.utils.SentryConstants.PRIVILEGE_PREFIX;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.sentry.core.common.validator.PrivilegeValidator;
import org.apache.sentry.core.common.validator.PrivilegeValidatorContext;
import org.apache.sentry.core.model.kafka.KafkaActionFactory;
import org.apache.sentry.core.model.kafka.KafkaAuthorizable;
import org.apache.sentry.core.model.kafka.Host;
import org.apache.sentry.core.model.kafka.KafkaModelAuthorizables;
import org.apache.shiro.config.ConfigurationException;

import com.google.common.collect.Lists;

/**
 * Validator for Kafka privileges.
 * Below are the requirements for a kafka privilege to be valid.
 * 1. Privilege must start with Host resource.
 * 2. Privilege must have at most one non Host resource, Cluster or Topic or ConsumerGroup, followed
 *    by Host resource.
 * 3. Privilege must end with exactly one action.
 */
public class KafkaPrivilegeValidator implements PrivilegeValidator {

  public static final String KafkaPrivilegeHelpMsg =
      "Invalid Kafka privilege." +
      " Kafka privilege must be of the form host=<HOST>-><RESOURCE>=<RESOURCE_NAME>->action=<ACTION>," +
      " where <HOST> can be '*' or any valid host name," +
      " <RESOURCE> can be one of " + Arrays.toString(getKafkaAuthorizablesExceptHost()) +
      " <RESOURCE_NAME> is name of the resource," +
      " <ACTION> can be one of " + Arrays.toString(KafkaActionFactory.KafkaActionType.values()) +
      ".";

  private static KafkaAuthorizable.AuthorizableType[] getKafkaAuthorizablesExceptHost() {
    final KafkaAuthorizable.AuthorizableType[] authorizableTypes = KafkaAuthorizable.AuthorizableType.values();
    List<KafkaAuthorizable.AuthorizableType> authorizableTypesWithoutHost = new ArrayList<>(authorizableTypes.length - 1);
    for (KafkaAuthorizable.AuthorizableType authorizableType: authorizableTypes) {
      if (!authorizableType.equals(KafkaAuthorizable.AuthorizableType.HOST)) {
        authorizableTypesWithoutHost.add(authorizableType);
      }
    }
    return authorizableTypesWithoutHost.toArray(new KafkaAuthorizable.AuthorizableType[authorizableTypesWithoutHost.size()]);
  }

  public KafkaPrivilegeValidator() {
  }

  @Override
  public void validate(PrivilegeValidatorContext context) throws ConfigurationException {
    List<String> splits = Lists.newArrayList();
    for (String section : AUTHORIZABLE_SPLITTER.split(context.getPrivilege())) {
      splits.add(section);
    }

    // Check privilege splits length is 2 or 3
    if (splits.size() < 2 || splits.size() > 3) {
      throw new ConfigurationException(KafkaPrivilegeHelpMsg);
    }

    // Check privilege starts with Host resource
    if (isAction(splits.get(0))) {
      throw new ConfigurationException("Kafka privilege can not start with an action.\n" + KafkaPrivilegeHelpMsg);
    }
    KafkaAuthorizable hostAuthorizable = KafkaModelAuthorizables.from(splits.get(0));
    if (hostAuthorizable == null) {
      throw new ConfigurationException("No Kafka authorizable found for " + splits.get(0) + "\n." + KafkaPrivilegeHelpMsg);
    }
    if (!(hostAuthorizable instanceof Host)) {
      throw new ConfigurationException("Kafka privilege must begin with host authorizable.\n" + KafkaPrivilegeHelpMsg);
    }

    // Check privilege has at most one non Host resource following Host resource
    if (splits.size() == 3) {
      if (isAction(splits.get(1))) {
        throw new ConfigurationException("Kafka privilege can have action only at the end of privilege.\n" + KafkaPrivilegeHelpMsg);
      }
      KafkaAuthorizable authorizable = KafkaModelAuthorizables.from(splits.get(1));
      if (authorizable == null) {
        throw new ConfigurationException("No Kafka authorizable found for " + splits.get(1) + "\n." + KafkaPrivilegeHelpMsg);
      }
      if (authorizable instanceof Host) {
        throw new ConfigurationException("Host authorizable can be specified just once in a Kafka privilege.\n" + KafkaPrivilegeHelpMsg);
      }
    }

    // Check privilege ends with exactly one valid action
    if (!isAction(splits.get(splits.size() - 1))) {
      throw new ConfigurationException("Kafka privilege must end with a valid action.\n" + KafkaPrivilegeHelpMsg);
    }
  }

  private boolean isAction(String privilegePart) {
    final String privilege = privilegePart.toLowerCase();
    final String action = privilege.replace(PRIVILEGE_PREFIX, "").toLowerCase();
    return privilege.startsWith(PRIVILEGE_PREFIX) &&
        KafkaActionFactory.getInstance().getActionByName(action) != null;
  }
}
