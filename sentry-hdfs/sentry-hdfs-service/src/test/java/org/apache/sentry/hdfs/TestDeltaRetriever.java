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
package org.apache.sentry.hdfs;

import org.apache.sentry.core.model.db.AccessConstants;
import org.apache.sentry.hdfs.service.thrift.TPrivilegePrincipal;
import org.apache.sentry.hdfs.service.thrift.TPrivilegePrincipalType;
import org.apache.sentry.hdfs.service.thrift.TPrivilegeChanges;
import org.apache.sentry.provider.db.service.model.MSentryPathChange;
import org.apache.sentry.provider.db.service.model.MSentryPermChange;
import org.apache.sentry.provider.db.service.persistent.SentryStore;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestDeltaRetriever {
  SentryStore sentryStoreMock;

  @Before
  public void setUp() {
    sentryStoreMock = Mockito.mock(SentryStore.class);
  }

  @Test
  public void testEmptyPathUpdatesRetrieveWhenNotPathChangesArePersisted() throws Exception {
    Mockito.when(sentryStoreMock.getMSentryPathChanges(Mockito.anyLong()))
        .thenReturn(Collections.<MSentryPathChange>emptyList());

    PathDeltaRetriever deltaRetriever = new PathDeltaRetriever(sentryStoreMock);
    List<PathsUpdate> pathsUpdates = deltaRetriever.retrieveDelta(1, 1);

    assertTrue(pathsUpdates.isEmpty());
  }

  @Test
  public void testDeltaPathUpdatesRetrievedWhenNewPathChangesArePersisted() throws Exception {
    PathDeltaRetriever deltaRetriever;
    List<PathsUpdate> pathsUpdates;

    List<MSentryPathChange> deltaPathChanges = Arrays.asList(
        new MSentryPathChange(1, "u1", new PathsUpdate(1, true)),
        new MSentryPathChange(2, "u2", new PathsUpdate(2, false))
    );

    Mockito.when(sentryStoreMock.getMSentryPathChanges(Mockito.anyLong()))
        .thenReturn(deltaPathChanges);

    deltaRetriever = new PathDeltaRetriever(sentryStoreMock);
    pathsUpdates = deltaRetriever.retrieveDelta(1, 3);

    assertEquals(2, pathsUpdates.size());
    assertEquals(1, pathsUpdates.get(0).getSeqNum());
    assertEquals(true, pathsUpdates.get(0).hasFullImage());
    assertEquals(3, pathsUpdates.get(0).getImgNum());
    assertEquals(2, pathsUpdates.get(1).getSeqNum());
    assertEquals(false, pathsUpdates.get(1).hasFullImage());
    assertEquals(3, pathsUpdates.get(1).getImgNum());
  }

  @Test
  public void testDeltaPermUpdatesRetrievedWhenOwnerPrivileges() throws Exception {
    PermDeltaRetriever deltaRetriever;
    List<PermissionsUpdate> permUpdates;

    Mockito.when(sentryStoreMock.getMSentryPermChanges(Mockito.anyLong())).
            thenAnswer(new Answer() {
              @Override
              public List<MSentryPermChange> answer(InvocationOnMock invocation)
                      throws Throwable {
                List<MSentryPermChange> permChanges = new ArrayList<>();
                PermissionsUpdate update = new PermissionsUpdate();
                update.addPrivilegeUpdate("obj1").putToAddPrivileges( new TPrivilegePrincipal(TPrivilegePrincipalType.ROLE,
                        "role1"), AccessConstants.OWNER);
                MSentryPermChange perm1 = new MSentryPermChange(1,update);
                permChanges.add(perm1);
                update = new PermissionsUpdate();
                update.addPrivilegeUpdate("obj1").putToAddPrivileges( new TPrivilegePrincipal(TPrivilegePrincipalType.USER,
                        "user1"), AccessConstants.OWNER);
                MSentryPermChange perm2 = new MSentryPermChange(2,update);
                permChanges.add(perm2);
                update = new PermissionsUpdate();
                update.addPrivilegeUpdate("obj1").putToDelPrivileges( new TPrivilegePrincipal(TPrivilegePrincipalType.ROLE,
                        "user1"), AccessConstants.OWNER);
                MSentryPermChange perm3 = new MSentryPermChange(2,update);
                permChanges.add(perm3);
                return permChanges;
              }
            });

    deltaRetriever = new PermDeltaRetriever(sentryStoreMock);
    permUpdates = deltaRetriever.retrieveDelta(0, 3);
    assertEquals(3, permUpdates.size());
    assertEquals(1, permUpdates.get(0).getSeqNum());

    for(PermissionsUpdate update : permUpdates) {
      for(TPrivilegeChanges priv : update.getPrivilegeUpdates()) {
        for(Map.Entry<TPrivilegePrincipal,String> privEntry : priv.getAddPrivileges().entrySet()) {
          assertEquals(AccessConstants.ALL, privEntry.getValue());
        }
        for(Map.Entry<TPrivilegePrincipal,String> privEntry : priv.getDelPrivileges().entrySet()) {
          assertEquals(AccessConstants.ALL, privEntry.getValue());
        }
      }
    }
  }
}
