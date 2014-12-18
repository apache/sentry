/**
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.sentry.provider.db.service.persistent;

import java.util.Set;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.SentryUserException;
import org.apache.sentry.core.model.db.AccessConstants;
import org.apache.sentry.provider.db.service.thrift.TSentryGrantOption;
import org.apache.sentry.provider.db.service.thrift.TSentryGroup;
import org.apache.sentry.provider.db.service.thrift.TSentryPrivilege;
import org.apache.sentry.provider.db.service.thrift.TStoreSnapshot;
import org.junit.Test;

import com.google.common.collect.Sets;

public class TestStoreSnapshot {

  static class DummyGroupMapper extends InMemSentryStore.GroupMapper {

    DummyGroupMapper(Configuration conf) {
      super(conf);
    }

    @Override
    protected Set<String> getGroupsForUser(String user)
        throws SentryUserException {
      return Sets.newHashSet("admin");
    }

    @Override
    protected Set<String> getAdminGroups() {
      return Sets.newHashSet("admin");
    }

    @Override
    protected boolean isInAdminGroup(Set<String> groups)
        throws SentryUserException {
      return true;
    }

  }
  @Test
  public void testSnapshot() throws Exception {
    Configuration conf = new Configuration(false);
    InMemSentryStore store1 = new InMemSentryStore(conf, new DummyGroupMapper(conf));
    store1.createSentryRole("role1");
    store1.alterSentryRoleAddGroups("grantor", "role1",
            Sets.newHashSet(
                new TSentryGroup("group1"),
                new TSentryGroup("group2")));
    TSentryPrivilege tPriv = new TSentryPrivilege("SERVER", "server1", AccessConstants.ALL);
    tPriv.setGrantOption(TSentryGrantOption.TRUE);
    store1.alterSentryRoleGrantPrivilege("grantor", "role1", tPriv);
    tPriv = new TSentryPrivilege("DB", "server1", AccessConstants.SELECT);
    tPriv.setDbName("db1");
    tPriv.setGrantOption(TSentryGrantOption.FALSE);
    store1.alterSentryRoleGrantPrivilege("grantor", "role1", tPriv);
    tPriv = new TSentryPrivilege("TABLE", "server1", AccessConstants.ALL);
    tPriv.setDbName("db1");
    tPriv.setTableName("table1");
    tPriv.setGrantOption(TSentryGrantOption.UNSET);
    store1.alterSentryRoleGrantPrivilege("grantor", "role1", tPriv);
    Set<TSentryPrivilege> allPrivs1 = store1.getAllTSentryPrivilegesByRoleName("role1");

    TStoreSnapshot snapshot = store1.toSnapshot();
    InMemSentryStore store2 = new InMemSentryStore(conf, new DummyGroupMapper(conf));
    store2.fromSnapshot(snapshot);
    Assert.assertEquals(
        Sets.newHashSet("group1", "group2"),
        store2.getGroupsForRole("role1"));
    Set<TSentryPrivilege> allPrivs2 = store2.getAllTSentryPrivilegesByRoleName("role1");
    Assert.assertEquals(allPrivs1, allPrivs2);
  }

}
