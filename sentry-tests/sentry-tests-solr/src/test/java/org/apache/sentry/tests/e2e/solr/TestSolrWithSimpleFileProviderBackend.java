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

import java.io.File;
import java.nio.charset.StandardCharsets;

import org.apache.commons.io.FileUtils;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestSolrWithSimpleFileProviderBackend extends AbstractSolrSentryTestCase {
  private static final Logger log = LoggerFactory.getLogger(TestSolrWithSimpleFileProviderBackend.class);
  protected static final String RESOURCES_DIR = "target" + File.separator + "test-classes" + File.separator + "solr";

  @BeforeClass
  public static void setupClass() throws Exception {
    // Configure file-based Sentry provider
    File sentrySiteXml = setupSentry();
    System.setProperty(SENTRY_SITE_LOC_SYSPROP, sentrySiteXml.getAbsolutePath());

    try {
      configureCluster(NUM_SERVERS)
        .withSecurityJson(TEST_PATH().resolve("security").resolve("security.json"))
        .addConfig("cloud-minimal", TEST_PATH().resolve("configsets").resolve("cloud-minimal").resolve("conf"))
        .addConfig("cloud-managed", TEST_PATH().resolve("configsets").resolve("cloud-managed").resolve("conf"))
        .addConfig("cloud-minimal_doc_level_security", TEST_PATH().resolve("configsets")
                      .resolve("cloud-minimal_doc_level_security").resolve("conf"))
        .configure();
      log.info("Successfully started Solr service");

    } catch (Exception ex) {
      log.error ("Unexpected exception while starting SolrCloud", ex);
      throw ex;
    }

    log.info("Successfully setup Solr with Sentry service");
  }

  private static void addPropertyToSentry(StringBuilder builder, String name, String value) {
    builder.append("<property>\n");
    builder.append("<name>").append(name).append("</name>\n");
    builder.append("<value>").append(value).append("</value>\n");
    builder.append("</property>\n");
  }

  private static File setupSentry() throws Exception {
    File sentrySite = File.createTempFile("sentry-site", "xml");
    sentrySite.deleteOnExit();
    File authProviderDir = new File(RESOURCES_DIR, "sentry");
    File authProviderName = new File (authProviderDir, "test-authz-provider.ini");

    // need to write sentry-site at execution time because we don't know
    // the location of sentry.solr.provider.resource beforehand
    StringBuilder sentrySiteData = new StringBuilder();
    sentrySiteData.append("<configuration>\n");
    addPropertyToSentry(sentrySiteData, "sentry.provider",
      "org.apache.sentry.provider.file.LocalGroupResourceAuthorizationProvider");
    addPropertyToSentry(sentrySiteData, "sentry.solr.provider.resource",
       authProviderName.getAbsolutePath());
    sentrySiteData.append("</configuration>\n");
    FileUtils.writeStringToFile(sentrySite,sentrySiteData.toString(), StandardCharsets.UTF_8);
    return sentrySite;
  }

  @Test
  public void testCollectionOperationsSuccess() throws Exception {
    String collectionName = "testCollectionOperations_s";
    createCollection("admin", collectionName, "cloud-minimal", NUM_SERVERS, 1);

    cleanSolrCollection("junit", collectionName);

    SolrInputDocument solrInputDoc = createSolrTestDoc();
    uploadSolrDoc("junit", collectionName, solrInputDoc);

    SolrDocumentList expectedDocs = expectedDocs(solrInputDoc);

    validateSolrDocCountAndContent(expectedDocs,
        getSolrDocs("junit", collectionName, ALL_DOCS));
  }

  @Test
  public void testCollectionOperationFailures() throws Exception {
    String collectionName = "testCollectionOperations_f";

    // junit user can not create a collection.
    adminUpdateActionFailure ("junit", collectionName);

    // admin user can create collection
    createCollection("admin", collectionName, "cloud-minimal", NUM_SERVERS, 1);

    SolrInputDocument solrInputDoc = createSolrTestDoc();

    // foo user can not update collection
    verifyCollectionUpdateFailure("foo", collectionName, solrInputDoc);

    // junit user can update collection
    uploadSolrDoc("junit", collectionName, solrInputDoc);

    SolrDocumentList expectedDocs = expectedDocs(solrInputDoc);

    // junit user can query collection
    validateSolrDocCountAndContent(expectedDocs,
        getSolrDocs("junit", collectionName, ALL_DOCS));

    // foo user can not query collection
    verifyCollectionQueryFailure("foo", collectionName, ALL_DOCS);
  }
}
