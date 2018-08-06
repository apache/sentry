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

import com.google.common.collect.Sets;
import junit.framework.Assert;
import org.apache.commons.lang.StringUtils;
import org.apache.sentry.core.model.db.AccessConstants;
import org.apache.sentry.hdfs.service.thrift.TPathChanges;
import org.apache.sentry.hdfs.service.thrift.TPrivilegeChanges;
import org.apache.sentry.hdfs.service.thrift.TPrivilegePrincipal;
import org.apache.sentry.hdfs.service.thrift.TPrivilegePrincipalType;
import org.apache.sentry.provider.db.service.persistent.PermissionsImage;
import org.apache.sentry.provider.db.service.persistent.SentryStore;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestImageRetriever {
  SentryStore sentryStoreMock;
  private static final String[] root = {"/"};

  @Before
  public void setUp() {
    sentryStoreMock = Mockito.mock(SentryStore.class);
  }

  @Ignore
  @Test
  public void testFullPathUpdatesRetrievedWhenNewImagesArePersisted() throws Exception {
    PathImageRetriever imageRetriever;
    PathsUpdate pathsUpdate;

    Map<String, Collection<String>> fullPathsImage = new HashMap<>();
    fullPathsImage.put("db1", Sets.newHashSet("/user/db1"));
    fullPathsImage.put("db1.table1", Sets.newHashSet("/user/db1/table1"));

    Mockito.when(sentryStoreMock.retrieveFullPathsImageUpdate(root))
        .thenReturn(new PathsUpdate(1, 1, true));

    imageRetriever = new PathImageRetriever(sentryStoreMock, root);
    pathsUpdate = imageRetriever.retrieveFullImage();

    assertEquals(1, pathsUpdate.getImgNum());
    assertEquals(1, pathsUpdate.getSeqNum());
    assertEquals(2, pathsUpdate.getPathChanges().size());
    assertTrue(comparePaths(fullPathsImage, pathsUpdate.getPathChanges()));
  }


  @Test
  public void testFullPermUpdatesRetrievedWithOwnerPrivileges() throws Exception {
    PermImageRetriever imageRetriever;
    PermissionsUpdate permUpdate;

    Mockito.when(sentryStoreMock.retrieveFullPermssionsImage()).
            thenAnswer(new Answer() {
              @Override
              public PermissionsImage answer(InvocationOnMock invocation)
                      throws Throwable {
                Map<String, Map<TPrivilegePrincipal, String>> privilegeMap = new HashMap<>();
                Map<TPrivilegePrincipal, String> privMap = new HashMap<>();
                privMap.put(new TPrivilegePrincipal(TPrivilegePrincipalType.ROLE, "role1"), AccessConstants.OWNER);
                privMap.put(new TPrivilegePrincipal(TPrivilegePrincipalType.USER, "user1"), AccessConstants.OWNER);
                privilegeMap.put("obj1", privMap);
                privilegeMap.put("obj2", privMap);
                return new PermissionsImage(new HashMap<>(), privilegeMap, 1L);
              }
            });

    imageRetriever = new PermImageRetriever(sentryStoreMock);
    permUpdate = imageRetriever.retrieveFullImage();
    Assert.assertNotNull(permUpdate);

    assertEquals(2, permUpdate.getPrivilegeUpdates().size());
    for(TPrivilegeChanges privUpdate : permUpdate.getPrivilegeUpdates()) {
      for(Map.Entry<TPrivilegePrincipal,String> priv : privUpdate.getAddPrivileges().entrySet()) {
        assertEquals(priv.getValue(), AccessConstants.ALL);
      }
    }
  }

  private boolean comparePaths(Map<String, Collection<String>> expected, List<TPathChanges> actual) {
    if (expected.size() != actual.size()) {
      return false;
    }

    for (TPathChanges pathChanges : actual) {
      if (!expected.containsKey(pathChanges.getAuthzObj())) {
        return false;
      }

      Collection<String> expectedPaths = expected.get(pathChanges.getAuthzObj());
      for (List<String> path : pathChanges.getAddPaths()) {
        if (!expectedPaths.contains(StringUtils.join(path, "/"))) {
          return false;
        }
      }
    }

    return true;
  }
}
