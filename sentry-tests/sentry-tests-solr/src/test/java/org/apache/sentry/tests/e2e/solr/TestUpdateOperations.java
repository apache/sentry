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

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope.Scope;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.solr.common.SolrInputDocument;

@ThreadLeakScope(Scope.NONE) // hdfs client currently leaks thread(s)
public class TestUpdateOperations extends AbstractSolrSentryTestBase {
  private static final Logger LOG = LoggerFactory
      .getLogger(TestUpdateOperations.class);
  private static final String COLLECTION_NAME = "sentryCollection";
  private static final List<Boolean> BOOLEAN_VALUES = Arrays.asList(new Boolean[]{true, false});

  @Override
  public void doTest() throws Exception {
    setupCollection(COLLECTION_NAME);
    ArrayList<String> testFailures = new ArrayList<String>();

    for (boolean query : BOOLEAN_VALUES) {
      for (boolean update : BOOLEAN_VALUES) {
        for (boolean all : BOOLEAN_VALUES) {
          String test_user = getUsernameForPermissions(COLLECTION_NAME, query, update, all);
          LOG.info("TEST_USER: " + test_user);

          try {
            if (all || update) {
              cleanSolrCollection(COLLECTION_NAME);
              SolrInputDocument solrInputDoc = createSolrTestDoc();
              verifyUpdatePass(test_user, COLLECTION_NAME, solrInputDoc);

              cleanSolrCollection(COLLECTION_NAME);
              uploadSolrDoc(COLLECTION_NAME, null);
              verifyDeletedocsPass(test_user, COLLECTION_NAME, false);
            } else {
              cleanSolrCollection(COLLECTION_NAME);
              SolrInputDocument solrInputDoc = createSolrTestDoc();
              verifyUpdateFail(test_user, COLLECTION_NAME, solrInputDoc);

              cleanSolrCollection(COLLECTION_NAME);
              uploadSolrDoc(COLLECTION_NAME, null);
              verifyDeletedocsFail(test_user, COLLECTION_NAME, false);
            }
          } catch (Throwable testException) {
            StringWriter stringWriter = new StringWriter();
            PrintWriter printWriter = new PrintWriter(stringWriter);
            testException.printStackTrace(printWriter);
            testFailures.add("\n\nTestFailure: User -> " + test_user + "\n"
                + stringWriter.toString());
          }
        }
      }
    }

    assertEquals("Total test failures: " + testFailures.size() + " \n\n"
        + testFailures.toString() + "\n\n\n", 0, testFailures.size());
  }
}
