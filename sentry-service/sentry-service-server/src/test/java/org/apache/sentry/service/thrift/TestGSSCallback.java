/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements.  See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the License.  You may obtain
 * a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.sentry.service.thrift;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.apache.sentry.service.common.ServiceConstants.ServerConfig;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class TestGSSCallback {

  private static final Configuration conf = new Configuration();
  private GSSCallback callBack;

  @Before
  public void setUp() {
    conf.set(ServerConfig.ALLOW_CONNECT, "hive");
    callBack = new GSSCallback(conf);
  }

  @Test
  public void testAllowConnectOnKerberosPrincipal() {
    //Test with ruleset not set
    String validPrincipal = "hive@GCE.CLOUDERA.COM";
    assertTrue("Authenticate valid user", callBack.allowConnect(validPrincipal));

    String invalidPrincipal = "impala@GCE.CLOUDERA.COM";
    assertFalse("Do not authenticate invalid user", callBack.allowConnect(invalidPrincipal));

    //Test with ruleset set to DEFAULT
    String ruleString = "DEFAULT";
    KerberosName.setRules(ruleString);

    assertTrue("Authenticate valid user", callBack.allowConnect(validPrincipal));
    assertFalse("Do not authenticate invalid user", callBack.allowConnect(invalidPrincipal));
  }

  @Test
  public void testAllowConnectWithRuleSet() {

    String ruleString = "RULE:[1:$1@$0](user1@TEST.REALM.COM)s/.*/hive/";
    KerberosName.setRules(ruleString);

    String validPrincipal = "user1@TEST.REALM.COM";
    assertTrue("Authenticate valid user", callBack.allowConnect(validPrincipal));

    //New rule for a different user
    ruleString = "RULE:[1:$1@$0](user2@TEST.REALM.COM)s/.*/solr/";
    KerberosName.setRules(ruleString);
    String invalidPrincipal1 = "user2@TEST.REALM.COM";
    assertFalse("Do not authenticate invalid user", callBack.allowConnect(invalidPrincipal1));
    String invalidPrincipal2 = "user3@TEST.REALM.COM";
    assertFalse("Do not authenticate invalid user", callBack.allowConnect(invalidPrincipal2));
  }

}
