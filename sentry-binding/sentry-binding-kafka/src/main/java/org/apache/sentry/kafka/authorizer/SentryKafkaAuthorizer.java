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

  private static Logger LOG =
      LoggerFactory.getLogger(SentryKafkaAuthorizer.class);

  KafkaAuthBinding binding;
  KafkaAuthConf kafkaAuthConf;

  String sentry_site = null;
  List<KafkaPrincipal> super_users = null;
  String kafkaServiceInstanceName = KafkaAuthConf.AuthzConfVars.getDefault(KafkaAuthConf.KAFKA_SERVICE_INSTANCE_NAME);

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
    throw new UnsupportedOperationException("Please use Sentry CLI to perform this action.");
  }

  @Override
  public boolean removeAcls(Set<Acl> acls, final Resource resource) {
    throw new UnsupportedOperationException("Please use Sentry CLI to perform this action.");
  }

  @Override
  public boolean removeAcls(final Resource resource) {
    throw new UnsupportedOperationException("Please use Sentry CLI to perform this action.");
  }

  @Override
  public Set<Acl> getAcls(Resource resource) {
    throw new UnsupportedOperationException("Please use Sentry CLI to perform this action.");
  }

  @Override
  public Map<Resource, Set<Acl>> getAcls(KafkaPrincipal principal) {
    throw new UnsupportedOperationException("Please use Sentry CLI to perform this action.");
  }

  @Override
  public Map<Resource, Set<Acl>> getAcls() {
    throw new UnsupportedOperationException("Please use Sentry CLI to perform this action.");
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
    LOG.info("Configuring Sentry KafkaAuthorizer: " + sentry_site);
    final KafkaAuthBindingSingleton instance = KafkaAuthBindingSingleton.getInstance();
    instance.configure(this.kafkaServiceInstanceName, sentry_site);
    this.binding = instance.getAuthBinding();
    this.kafkaAuthConf = instance.getKafkaAuthConf();
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
}
