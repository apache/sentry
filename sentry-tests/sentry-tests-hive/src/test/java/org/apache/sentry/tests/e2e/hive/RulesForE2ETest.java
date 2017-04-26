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

package org.apache.sentry.tests.e2e.hive;

import org.junit.Rule;
import org.junit.ClassRule;
import org.junit.internal.runners.statements.FailOnTimeout;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class contains rules to run e2e tests.
 * The way to use a rule is to create an annotation class;
 * then add the rule specifically for test classes annotated by that particular class.
 */
public abstract class RulesForE2ETest {
  private static final Logger LOGGER = LoggerFactory
      .getLogger(RulesForE2ETest.class);

  final static int slowTestClassRunTime = 900000; //millis, each test runs less than 900s (or 15m)
  final static int avgTestClassRunTime = 600000; //millis, each test runs less than 600s (or 10m)
  final static int slowTestRunTime = 600000; //millis, each test runs less than 600s (or 10m)
  final static int avgTestRunTime = 180000; //millis, each test runs less than 180s (or 3m)

  @ClassRule
  public final static TestRule timeoutClass = new TestRule() {
    @Override
    public Statement apply(Statement base, Description description) {
      if (description != null && description.getAnnotation(SlowE2ETest.class) != null) {
        LOGGER.info("SlowE2ETest class time out is configured as " + slowTestClassRunTime + " s.");
        return new FailOnTimeout(base, slowTestClassRunTime);
      }
      LOGGER.info("Average class time out is configured as " + avgTestClassRunTime + " s.");
      return new FailOnTimeout(base, avgTestClassRunTime); //millis, each test runs less than 600s (or 10m)
    }
  };

  @Rule
  public final TestRule timeout = new TestRule() {
    @Override
    public Statement apply(Statement base, Description description) {
      if (description != null && description.getAnnotation(SlowE2ETest.class) != null) {
        LOGGER.info("SlowE2ETest time out is configured as " + slowTestRunTime + " s.");
        return new FailOnTimeout(base, slowTestRunTime);
      }
      LOGGER.info("Average test time out is configured as " + avgTestRunTime + " s.");
      return new FailOnTimeout(base, avgTestRunTime);
    }
  };
}
