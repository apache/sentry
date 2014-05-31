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
package org.apache.sentry.provider.common;

import static org.junit.Assert.assertSame;

import java.util.Set;
import java.util.List;

import org.apache.sentry.core.common.Authorizable;
import org.apache.sentry.core.common.SentryConfigurationException;
import org.apache.sentry.core.common.ActiveRoleSet;
import org.apache.sentry.policy.common.PrivilegeFactory;
import org.apache.sentry.policy.common.PolicyEngine;
import org.apache.sentry.provider.common.GroupMappingService;
import org.apache.sentry.provider.common.ResourceAuthorizationProvider;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

public class TestGetGroupMapping {

  private static class TestResourceAuthorizationProvider extends ResourceAuthorizationProvider {
    public TestResourceAuthorizationProvider(PolicyEngine policy,
      GroupMappingService groupService) {
      super(policy, groupService);
    }
  };

  @Test
  public void testResourceAuthorizationProvider() {
    final Set<String> set = Sets.newHashSet("a", "b", "c");
    GroupMappingService mappingService = new GroupMappingService() {
      @Override
      public Set<String> getGroups(String user) { return set; }
    };
    PolicyEngine policyEngine = new PolicyEngine() {
      @Override
      public PrivilegeFactory getPrivilegeFactory() { return null; }

      @Override
      public ImmutableSet<String> getAllPrivileges(Set<String> groups,
          ActiveRoleSet roleSet) throws SentryConfigurationException {
        return getPrivileges(groups, roleSet, null);
      }

      @Override
      public ImmutableSet<String> getPrivileges(Set<String> groups, ActiveRoleSet roleSet, Authorizable... authorizableHierarchy) {
        return ImmutableSet.of();
      }

      @Override
      public void validatePolicy(boolean strictValidation)
          throws SentryConfigurationException {
        return;
      }

      @Override
      public void close() {}
    };

    TestResourceAuthorizationProvider authProvider =
      new TestResourceAuthorizationProvider(policyEngine, mappingService);
    assertSame(authProvider.getGroupMapping(), mappingService);
  }
}
