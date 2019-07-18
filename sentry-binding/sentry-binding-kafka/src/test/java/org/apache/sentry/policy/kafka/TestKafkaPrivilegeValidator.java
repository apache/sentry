/*
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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.sentry.policy.kafka;

import junit.framework.Assert;

import org.apache.sentry.core.common.validator.PrivilegeValidatorContext;
import org.apache.sentry.core.model.kafka.validator.KafkaPrivilegeValidator;
import org.apache.shiro.config.ConfigurationException;
import org.junit.Test;

public class TestKafkaPrivilegeValidator {
  @Test
  public void testOnlyHostResource() {
    KafkaPrivilegeValidator kafkaPrivilegeValidator = new KafkaPrivilegeValidator();
    try {
      kafkaPrivilegeValidator.validate(new PrivilegeValidatorContext("host=host1"));
    } catch (ConfigurationException ex) {
      Assert.assertEquals(KafkaPrivilegeValidator.KafkaPrivilegeHelpMsg, ex.getMessage());
    }
  }

  @Test
  public void testWithoutHostResource() throws Exception {
    KafkaPrivilegeValidator kafkaPrivilegeValidator = new KafkaPrivilegeValidator();
    testHostResourceIsChecked(kafkaPrivilegeValidator, "cluster=kafka-cluster->action=read");
    testHostResourceIsChecked(kafkaPrivilegeValidator, "topic=t1->action=read");
    testHostResourceIsChecked(kafkaPrivilegeValidator, "consumergroup=g1->action=read");
  }

  private void testHostResourceIsChecked(KafkaPrivilegeValidator kafkaPrivilegeValidator, String privilege) {
    try {
      kafkaPrivilegeValidator.validate(new PrivilegeValidatorContext(privilege));
      Assert.fail("Expected ConfigurationException");
    } catch (ConfigurationException ex) {
      Assert.assertEquals("Kafka privilege must begin with host authorizable.\n" + KafkaPrivilegeValidator.KafkaPrivilegeHelpMsg, ex.getMessage());
    }
  }

  @Test
  public void testValidPrivileges() throws Exception {
    KafkaPrivilegeValidator kafkaPrivilegeValidator = new KafkaPrivilegeValidator();
    try {
      kafkaPrivilegeValidator.validate(new PrivilegeValidatorContext("host=host1->cluster=kafka-cluster->action=read"));
    } catch (ConfigurationException ex) {
      Assert.fail("Not expected ConfigurationException");
    }
    try {
      kafkaPrivilegeValidator.validate(new PrivilegeValidatorContext("host=host1->topic=t1->action=read"));
    } catch (ConfigurationException ex) {
      Assert.fail("Not expected ConfigurationException");
    }
    try {
      kafkaPrivilegeValidator.validate(new PrivilegeValidatorContext("host=host1->consumergroup=g1->action=read"));
    } catch (ConfigurationException ex) {
      Assert.fail("Not expected ConfigurationException");
    }
    try {
      kafkaPrivilegeValidator.validate(new PrivilegeValidatorContext("host=host1->transactionalid=t1->action=write"));
    } catch (ConfigurationException ex) {
      Assert.fail("Not expected ConfigurationException");
    }
  }

  @Test
  public void testInvalidHostResource() throws Exception {
    KafkaPrivilegeValidator kafkaPrivilegeValidator = new KafkaPrivilegeValidator();
    try {
      kafkaPrivilegeValidator.validate(new PrivilegeValidatorContext("hhost=host1->cluster=kafka-cluster->action=read"));
      Assert.fail("Expected ConfigurationException");
    } catch (ConfigurationException ex) {
    }
  }

  @Test
  public void testInvalidClusterResource() throws Exception {
    KafkaPrivilegeValidator kafkaPrivilegeValidator = new KafkaPrivilegeValidator();
    try {
      kafkaPrivilegeValidator.validate(new PrivilegeValidatorContext("host=host1->clluster=kafka-cluster->action=read"));
      Assert.fail("Expected ConfigurationException");
    } catch (ConfigurationException ex) {
    }
  }

  @Test
  public void testInvalidTopicResource() throws Exception {
    KafkaPrivilegeValidator kafkaPrivilegeValidator = new KafkaPrivilegeValidator();
    try {
      kafkaPrivilegeValidator.validate(new PrivilegeValidatorContext("host=host1->ttopic=t1->action=read"));
      Assert.fail("Expected ConfigurationException");
    } catch (ConfigurationException ex) {
    }
  }

  @Test
  public void testInvalidTransactionalIdResource() throws Exception {
    KafkaPrivilegeValidator kafkaPrivilegeValidator = new KafkaPrivilegeValidator();
    try {
      kafkaPrivilegeValidator.validate(new PrivilegeValidatorContext("host=host1->transationalid=t1->action=write"));
      Assert.fail("Expected ConfigurationException");
    } catch (ConfigurationException ex) {
    }
  }

  @Test
  public void testInvalidConsumerGroupResource() throws Exception {
    KafkaPrivilegeValidator kafkaPrivilegeValidator = new KafkaPrivilegeValidator();
    try {
      kafkaPrivilegeValidator.validate(new PrivilegeValidatorContext("host=host1->coonsumergroup=g1->action=read"));
      Assert.fail("Expected ConfigurationException");
    } catch (ConfigurationException ex) {
    }
  }

  @Test
  public void testPrivilegeMustHaveExcatlyOneHost() {
    KafkaPrivilegeValidator kafkaPrivilegeValidator = new KafkaPrivilegeValidator();
    try {
      kafkaPrivilegeValidator.validate(new PrivilegeValidatorContext("host=host1->host=host2->action=read"));
      Assert.fail("Multiple Host resources are not allowed within a Kafka privilege.");
    } catch (ConfigurationException ex) {
      Assert.assertEquals("Host authorizable can be specified just once in a Kafka privilege.\n" + KafkaPrivilegeValidator.KafkaPrivilegeHelpMsg, ex.getMessage());
    }
  }

  @Test
  public void testPrivilegeCanNotStartWithAction() {
    KafkaPrivilegeValidator kafkaPrivilegeValidator = new KafkaPrivilegeValidator();
    try {
      kafkaPrivilegeValidator.validate(new PrivilegeValidatorContext("action=write->host=host1->topic=t1"));
      Assert.fail("Kafka privilege can not start with an action.");
    } catch (ConfigurationException ex) {
      Assert.assertEquals("Kafka privilege can not start with an action.\n" + KafkaPrivilegeValidator.KafkaPrivilegeHelpMsg, ex.getMessage());
    }
  }

  @Test
  public void testPrivilegeWithMoreParts() {
    KafkaPrivilegeValidator kafkaPrivilegeValidator = new KafkaPrivilegeValidator();
    try {
      kafkaPrivilegeValidator.validate(new PrivilegeValidatorContext("host=host1->topic=t1->consumergroup=cg1->action=read"));
      Assert.fail("Kafka privilege can have one Host authorizable, at most one non Host authorizable and one action.");
    } catch (ConfigurationException ex) {
      Assert.assertEquals(KafkaPrivilegeValidator.KafkaPrivilegeHelpMsg, ex.getMessage());
    }
    try {
      kafkaPrivilegeValidator.validate(new PrivilegeValidatorContext("host=host1->topic=t1->transactionalid=t1->action=read"));
      Assert.fail("Kafka privilege can have one Host authorizable, at most one non Host authorizable and one action.");
    } catch (ConfigurationException ex) {
      Assert.assertEquals(KafkaPrivilegeValidator.KafkaPrivilegeHelpMsg, ex.getMessage());
    }
  }

  @Test
  public void testPrivilegeNotEndingWithAction() {
    KafkaPrivilegeValidator kafkaPrivilegeValidator = new KafkaPrivilegeValidator();
    try {
      kafkaPrivilegeValidator.validate(new PrivilegeValidatorContext("host=host1->topic=t1->consumergroup=cg1"));
      Assert.fail("Kafka privilege must end with a valid action.");
    } catch (ConfigurationException ex) {
      Assert.assertEquals("Kafka privilege must end with a valid action.\n" + KafkaPrivilegeValidator.KafkaPrivilegeHelpMsg, ex.getMessage());
    }
  }

  @Test
  public void testPrivilegeNotEndingWithValidAction() {
    KafkaPrivilegeValidator kafkaPrivilegeValidator = new KafkaPrivilegeValidator();
    try {
      kafkaPrivilegeValidator.validate(new PrivilegeValidatorContext("host=host1->topic=t1->action=bla"));
      Assert.fail("Kafka privilege must end with a valid action.");
    } catch (ConfigurationException ex) {
      Assert.assertEquals("Kafka privilege must end with a valid action.\n" + KafkaPrivilegeValidator.KafkaPrivilegeHelpMsg, ex.getMessage());
    }
  }
}
