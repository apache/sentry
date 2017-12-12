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
import org.apache.commons.lang.StringUtils;
import org.apache.sentry.hdfs.service.thrift.TPathChanges;
import org.apache.sentry.provider.db.service.persistent.SentryStore;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;

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
