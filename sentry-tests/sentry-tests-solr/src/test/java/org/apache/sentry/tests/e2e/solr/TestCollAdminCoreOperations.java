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
package org.apache.sentry.tests.e2e.solr;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;

import org.apache.solr.common.params.CollectionParams.CollectionAction;

import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class TestCollAdminCoreOperations extends AbstractSolrSentryTestBase {

  private static final Logger LOG = LoggerFactory
      .getLogger(TestCollAdminCoreOperations.class);
  private static final String ADMIN_COLLECTION_NAME = "admin";
  private static final String TEST_COLLECTION_NAME = "sentryCollection";
  private static final List<Boolean> BOOLEAN_VALUES = Arrays.asList(new Boolean[]{true, false});
  private static final String DEFAULT_COLLECTION = "collection1";

  /**
   * Maximum number of combinations that will be tested by this class.
   */
  private static final int MAX_TEST_RUNS = 64;

  /**
   * Default number of combinations to be tested:15.
   */
  private static int NUM_TESTS_TO_RUN = 15;

  @Test
  public void testCollAdminCoreOperations() throws Exception {
    String maxTestsToRun = System.getProperty("sentry.solr.e2e.maxTestsToRun");
    if (maxTestsToRun != null) {
      if (maxTestsToRun.compareToIgnoreCase("all") == 0) {
        NUM_TESTS_TO_RUN = MAX_TEST_RUNS;
      } else {
        NUM_TESTS_TO_RUN = Integer.parseInt(maxTestsToRun);
        if (NUM_TESTS_TO_RUN > MAX_TEST_RUNS) {
          NUM_TESTS_TO_RUN = MAX_TEST_RUNS;
        }
      }
    }

    Random randomNum = new Random();
    HashSet<Integer> iterationSet = new HashSet<Integer>();
    while (iterationSet.size() < NUM_TESTS_TO_RUN) {
      iterationSet.add(randomNum.nextInt(MAX_TEST_RUNS));
    }
    int testCounter = 0;

    ArrayList<String> testFailures = new ArrayList<String>();
    // Upload configs to ZK
    uploadConfigDirToZk(RESOURCES_DIR + File.separator + DEFAULT_COLLECTION
        + File.separator + "conf");
    for (boolean admin_query : BOOLEAN_VALUES) {
      for (boolean admin_update : BOOLEAN_VALUES) {
        for (boolean admin_all : BOOLEAN_VALUES) {
          String admin_test_user = getUsernameForPermissions(ADMIN_COLLECTION_NAME, admin_query, admin_update, admin_all);

          for (boolean coll_query : BOOLEAN_VALUES) {
            for (boolean coll_update : BOOLEAN_VALUES) {
              for (boolean coll_all : BOOLEAN_VALUES) {
                if (!iterationSet.contains(testCounter)) {
                  testCounter = testCounter + 1;
                  continue;
                }
                testCounter = testCounter + 1;

                String coll_test_user = null;
                try {
                  coll_test_user = admin_test_user
                      .concat("__")
                      .concat(getUsernameForPermissions(TEST_COLLECTION_NAME, coll_query, coll_update, coll_all));
                  LOG.info("TEST_USER: " + coll_test_user);

                  // Setup the environment
                  deleteCollection(TEST_COLLECTION_NAME);

                  if ((admin_all || admin_update) && (coll_all || coll_update)) {
                    verifyCollectionAdminOpPass(coll_test_user,
                                                CollectionAction.CREATE,
                                                TEST_COLLECTION_NAME);
                    verifyCollectionAdminOpPass(coll_test_user,
                                                CollectionAction.RELOAD,
                                                TEST_COLLECTION_NAME);
                    verifyCollectionAdminOpPass(coll_test_user,
                                                CollectionAction.DELETE,
                                                TEST_COLLECTION_NAME);
                  } else {
                    verifyCollectionAdminOpFail(coll_test_user,
                                                CollectionAction.CREATE,
                                                TEST_COLLECTION_NAME);
                    // In-order to test RELOAD, DELETE for the current user,
                    // we need to setup a collection.
                    setupCollection(TEST_COLLECTION_NAME);
                    verifyCollectionAdminOpFail(coll_test_user,
                                                CollectionAction.RELOAD,
                                                TEST_COLLECTION_NAME);
                    verifyCollectionAdminOpFail(coll_test_user,
                                                CollectionAction.DELETE,
                                                TEST_COLLECTION_NAME);
                  }
                } catch (Throwable testException) {
                  StringWriter stringWriter = new StringWriter();
                  PrintWriter printWriter = new PrintWriter(stringWriter);
                  testException.printStackTrace(printWriter);
                  testFailures.add("\n\nTestFailure: User -> " + coll_test_user + "\n"
                      + stringWriter.toString());
                }
              }
            }
          }
        }
      }
    }

    assertEquals("Total test failures: " + testFailures.size() + " \n\n"
        + testFailures.toString() + "\n\n\n", 0, testFailures.size());
  }
}
