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
package org.apache.sentry.kafka.authorizer;

import kafka.network.RequestChannel;
import kafka.security.auth.Acl;
import kafka.security.auth.Authorizer;
import kafka.security.auth.Operation;
import kafka.security.auth.Resource;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.sentry.kafka.binding.KafkaAuthBinding;
import org.apache.sentry.kafka.binding.KafkaAuthBindingSingleton;
import org.apache.sentry.kafka.conf.KafkaAuthConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.collection.immutable.Map;
import scala.collection.immutable.Set;

import java.util.ArrayList;
import java.util.List;


public class SentryKafkaAuthorizer implements Authorizer {

  private final static Logger LOG = LoggerFactory.getLogger(SentryKafkaAuthorizer.class);
  private final static String INSTANCE_NAME = KafkaAuthConf.AuthzConfVars.getDefault(KafkaAuthConf.KAFKA_SERVICE_INSTANCE_NAME);

  private KafkaAuthBinding binding;
  private String kafkaServiceInstanceName = INSTANCE_NAME;
  private String requestorName = KafkaAuthConf.AuthzConfVars.getDefault(KafkaAuthConf.KAFKA_SERVICE_USER_NAME);

  String sentry_site = null;
  List<KafkaPrincipal> super_users = null;

  public SentryKafkaAuthorizer() {
  }

  @Override
  public boolean authorize(RequestChannel.Session session, Operation operation,
                           Resource resource) {
    LOG.debug("Authorizing Session: " + session + " for Operation: " + operation + " on Resource: " + resource);
    final KafkaPrincipal user = session.principal();
    if (isSuperUser(user)) {
      LOG.debug("Allowing SuperUser: " + user + " in " + session + " for Operation: " + operation + " on Resource: " + resource);
      return true;
    }
    LOG.debug("User: " + user + " is not a SuperUser");
    return binding.authorize(session, operation, resource);
}

  @Override
  public void addAcls(Set<Acl> acls, final Resource resource) {
    binding.addAcls(acls, resource);
  }

  @Override
  public boolean removeAcls(Set<Acl> acls, final Resource resource) {
    return binding.removeAcls(acls, resource);
  }

  @Override
  public boolean removeAcls(final Resource resource) {
    return binding.removeAcls(resource);
  }

  @Override
  public Set<Acl> getAcls(Resource resource) {
    return binding.getAcls(resource);
  }

  @Override
  public Map<Resource, Set<Acl>> getAcls(KafkaPrincipal principal) {
    return binding.getAcls(principal);
  }

  @Override
  public Map<Resource, Set<Acl>> getAcls() {
    return binding.getAcls();
  }

  @Override
  public void close() {
  }

  @Override
  public void configure(java.util.Map<String, ?> configs) {
    final Object sentryKafkaSiteUrlConfig = configs.get(KafkaAuthConf.SENTRY_KAFKA_SITE_URL);
    if (sentryKafkaSiteUrlConfig != null) {
      this.sentry_site = sentryKafkaSiteUrlConfig.toString();
    }
    final Object kafkaSuperUsersConfig = configs.get(KafkaAuthConf.KAFKA_SUPER_USERS);
    if (kafkaSuperUsersConfig != null) {
      getSuperUsers(kafkaSuperUsersConfig.toString());
    }
    final Object kafkaServiceInstanceName = configs.get(KafkaAuthConf.KAFKA_SERVICE_INSTANCE_NAME);
    if (kafkaServiceInstanceName != null) {
      this.kafkaServiceInstanceName = kafkaServiceInstanceName.toString();
    }
    final Object kafkaServiceUserName = configs.get(KafkaAuthConf.KAFKA_SERVICE_USER_NAME);
    if (kafkaServiceUserName != null) {
      this.requestorName = kafkaServiceUserName.toString();
    }
    LOG.info("Configuring Sentry KafkaAuthorizer: " + sentry_site);
    final KafkaAuthBindingSingleton instance = KafkaAuthBindingSingleton.getInstance();
    instance.configure(this.kafkaServiceInstanceName, this.requestorName, sentry_site, configs);
    this.binding = instance.getAuthBinding();
  }

  private void getSuperUsers(String kafkaSuperUsers) {
    super_users = new ArrayList<>();
    String[] superUsers = kafkaSuperUsers.split(";");
    for (String superUser : superUsers) {
      if (!superUser.isEmpty()) {
        final String trimmedUser = superUser.trim();
        super_users.add(KafkaPrincipal.fromString(trimmedUser));
        LOG.debug("Adding " + trimmedUser + " to list of Kafka SuperUsers.");
      }
    }
  }

  private boolean isSuperUser(KafkaPrincipal user) {
    if (super_users != null) {
      for (KafkaPrincipal superUser : super_users) {
        if (superUser.equals(user)) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * This is not used by Kafka, however as role is a Sentry centric entity having some mean to perform role CRUD will be required.
   * This method will be used by a Sentry-Kafka cli that will allow users to perform CRUD of roles and adding roles to groups.
   */
  public void addRole(String role) {
    binding.addRole(role);
  }

  /**
   * This is not used by Kafka, however as role is a Sentry centric entity having some mean to add role to groups will be required.
   * This method will be used by a Sentry-Kafka cli that will allow users to perform CRUD of roles and adding roles to groups.
   */
  public void addRoleToGroups(String role, java.util.Set<String> groups) {
    binding.addRoleToGroups(role, groups);
  }

  /**
   * This is not used by Kafka, however as role is a Sentry centric entity having some mean to perform role CRUD will be required.
   * This method will be used by a Sentry-Kafka cli that will allow users to perform CRUD of roles and adding roles to groups.
   */
  public void dropAllRoles() {
    binding.dropAllRoles();
  }
}
