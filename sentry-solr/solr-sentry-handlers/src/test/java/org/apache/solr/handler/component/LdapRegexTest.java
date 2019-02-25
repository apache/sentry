package org.apache.solr.handler.component;
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

import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import org.apache.commons.lang.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public class LdapRegexTest {

  @Test
  public void regexSimpleTest() {
    Multimap<String, String> userAttributes = LinkedListMultimap.create();

    userAttributes.put("attr1", "test1");
    userAttributes.put("attr2", "test2");
    userAttributes.put("attr3", "test3");
    userAttributes.put("attr4", "test4");

    Set<String> expectedResults = new HashSet<>();
    expectedResults.add("test1");
    expectedResults.add("test2");
    expectedResults.add("test3");
    expectedResults.add("test4");

    String regex = "[A-Za-z0-9]*";

    Collection<String> results = runRegexesForAttributes(userAttributes, regex);

    Assert.assertTrue(results.containsAll(expectedResults));
    Assert.assertTrue(expectedResults.containsAll(results));

  }

  @Test
  public void regexRoleIdTest() {
    Multimap<String, String> userAttributes = LinkedListMultimap.create();

    userAttributes.put("attr1", "some_prefix_r1234");
    userAttributes.put("attr2", "some_prefix_r2345");

    Set<String> expectedResults = new HashSet<>();
    expectedResults.add("1234");
    expectedResults.add("2345");

    String regex = "(some_prefix_r([0-9]+))";

    Collection<String> results = runRegexesForAttributes(userAttributes, regex);

    Assert.assertTrue(results.containsAll(expectedResults));
    Assert.assertTrue(expectedResults.containsAll(results));

  }

  @Test
  public void regexDNTest() {
    Multimap<String, String> userAttributes = LinkedListMultimap.create();

    userAttributes.put("attr1", "CN=JohnDoe, OU=People, DC=apache, DC=com");
    userAttributes.put("attr2", "O=Apache, CN=JoeBloggs, OU=People, DC=apache, DC=com");

    Set<String> expectedResults = new HashSet<>();
    expectedResults.add("JohnDoe");
    expectedResults.add("JoeBloggs");

    String regex = "(CN=([A-Za-z0-9\\-\\_]+),)";

    Collection<String> results = runRegexesForAttributes(userAttributes, regex);

    Assert.assertTrue(results.containsAll(expectedResults));
    Assert.assertTrue(expectedResults.containsAll(results));

  }

  @Test
  public void regexComplexTest() {
    Multimap<String, String> userAttributes = LinkedListMultimap.create();

    userAttributes.put("attr1", "abc123");
    userAttributes.put("attr2", "O=Sentry, CN=a_b_thisisatest, OU=People, DC=apache, DC=com");
    userAttributes.put("attr3", "O=Sentry, CN=a_b_test_thisisatestnumber2, OU=People, DC=apache, DC=com");

    Set<String> expectedResults = new HashSet<>();
    expectedResults.add("abc123");
    expectedResults.add("thisisatest");
    expectedResults.add("thisisatestnumber2");

    String regex = "(^[A-Za-z0-9]+$)|(CN=(a_b_(?:test_)?([A-Za-z0-9\\-\\_]{2,})),)";

    Collection<String> results = runRegexesForAttributes(userAttributes, regex);

    Assert.assertTrue(results.containsAll(expectedResults));
    Assert.assertTrue(expectedResults.containsAll(results));
  }

  private Collection<String> runRegexesForAttributes(Multimap<String, String> userAttributes, String regex) {
    String fieldName = "test1";
    String ldapAttributeNames = StringUtils.join(userAttributes.keySet().toArray(), ",");
    String filterType = "AND";
    boolean acceptEmpty = false;
    String allUsersValue = "N/A";
    String extraOpts = "";

    FieldToAttributeMapping mapping = new FieldToAttributeMapping(fieldName, ldapAttributeNames, filterType, acceptEmpty, allUsersValue, regex, extraOpts);

    return SolrAttrBasedFilter.getUserAttributesForField(userAttributes, mapping);

  }

}
