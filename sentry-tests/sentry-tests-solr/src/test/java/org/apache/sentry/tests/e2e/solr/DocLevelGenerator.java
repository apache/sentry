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

import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.junit.Assert;

import java.util.ArrayList;

public class DocLevelGenerator {
  private String collection;
  private String authField;

  public DocLevelGenerator(String collection, String authField) {
    this.collection = collection;
    this.authField = authField;
  }

  /**
   * Generates docs according to the following parameters:
   *
   * @param client Solr client to use
   * @param numDocs number of documents to generate
   * @param evenDocsToken every even number doc gets this token added to the authField
   * @param oddDocsToken every odd number doc gets this token added to the authField
   * @param extraAuthFieldsCount generates this number of bogus entries in the authField
   */
  public void generateDocs(CloudSolrClient client, int numDocs, String evenDocsToken, String oddDocsToken, int extraAuthFieldsCount) throws Exception {

    // create documents
    ArrayList<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
    for (int i = 0; i < numDocs; ++i) {
      SolrInputDocument doc = new SolrInputDocument();
      String iStr = Long.toString(i);
      doc.addField("id", iStr);
      doc.addField("description", "description" + iStr);

      // put some bogus tokens in
      for (int k = 0; k < extraAuthFieldsCount; ++k) {
        doc.addField(authField, authField + Long.toString(k));
      }
      // even docs get evenDocsToken, odd docs get oddDocsToken
      if (i % 2 == 0) {
        doc.addField(authField, evenDocsToken);
      } else {
        doc.addField(authField, oddDocsToken);
      }
      // add a token to all docs so we can check that we can get all
      // documents returned
      doc.addField(authField, "doclevel_role");

      docs.add(doc);
    }

    client.add(collection, docs);
    client.commit(collection, true, true);
  }

  public void generateDocsForSubsetQueries(CloudSolrClient client, int numDocs, int numTokens, String tokenPrefix) throws Exception {

    Assert.assertTrue("Num Tokens should be divisible by numTokens",numDocs % numTokens == 0);

    // create documents
    ArrayList<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
    for (int i = 0; i < numDocs; ++i) {
      int tokenCount = 0;
      SolrInputDocument doc = new SolrInputDocument();
      String iStr = Long.toString(i);
      doc.addField("id", iStr);
      doc.addField("description", "description" + iStr);

      for (int j=0; j < numTokens; j++) {
        if ((i & 1 << j ) == 1 << j) {
          doc.addField(authField, tokenPrefix + j);
          tokenCount++;
        }
      }
      doc.addField("sentry_auth_count", tokenCount);
System.out.println(doc);
      docs.add(doc);
    }

    client.add(collection, docs);
    client.commit(collection,true, true);
  }

}
