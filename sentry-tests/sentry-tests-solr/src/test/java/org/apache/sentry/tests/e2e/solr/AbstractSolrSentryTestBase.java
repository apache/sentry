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
import java.io.IOException;
import java.util.Comparator;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.cloud.AbstractFullDistribZkTestBase;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.servlet.SolrDispatchFilter;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractSolrSentryTestBase extends AbstractFullDistribZkTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractSolrSentryTestBase.class);
  private static MiniDFSCluster dfsCluster;
  private static SortedMap<Class, String> extraRequestFilters;

  private static void addPropertyToSentry(StringBuilder builder, String name, String value) {
    builder.append("<property>\n");
    builder.append("<name>").append(name).append("</name>\n");
    builder.append("<value>").append(value).append("</value>\n");
    builder.append("</property>\n");
  }

  public static File setupSentry() throws Exception {
    File sentrySite = File.createTempFile("sentry-site", "xml");
    sentrySite.deleteOnExit();
    File authProviderDir = new File(SolrTestCaseJ4.TEST_HOME(), "sentry");
    String authProviderName = "test-authz-provider.ini";
    FileSystem clusterFs = dfsCluster.getFileSystem();
    clusterFs.copyFromLocalFile(false,
      new Path(authProviderDir.toString(), authProviderName),
      new Path(authProviderName));

    // need to write sentry-site at execution time because we don't know
    // the location of sentry.solr.provider.resource beforehand
    StringBuilder sentrySiteData = new StringBuilder();
    sentrySiteData.append("<configuration>\n");
    addPropertyToSentry(sentrySiteData, "sentry.provider",
      "org.apache.sentry.provider.file.LocalGroupResourceAuthorizationProvider");
    addPropertyToSentry(sentrySiteData, "sentry.solr.provider.resource",
       clusterFs.getWorkingDirectory() + File.separator + authProviderName);
    sentrySiteData.append("</configuration>\n");
    FileUtils.writeStringToFile(sentrySite,sentrySiteData.toString());
    return sentrySite;
  }

  @BeforeClass
  public static void beforeTestSimpleSolrEndToEnd() throws Exception {
    dfsCluster = HdfsTestUtil.setupClass(new File(TEMP_DIR,
      AbstractSolrSentryTestBase.class.getName() + "_"
        + System.currentTimeMillis()).getAbsolutePath());
    File sentrySite = setupSentry();
    System.setProperty("solr.authorization.sentry.site", sentrySite.toURI().toURL().toString().substring("file:".length()));
    System.setProperty("solr.hdfs.home", dfsCluster.getURI().toString() + "/solr");
    extraRequestFilters = new TreeMap<Class, String>(new Comparator<Class>() {
      // There's only one class, make this as simple as possible
      public int compare(Class o1, Class o2) {
        return 0;
      }

      public boolean equals(Object obj) {
        return true;
      }
    });
    extraRequestFilters.put(JunitAuthenticationFilter.class, "*");
  }

  @AfterClass
  public static void teardownClass() throws Exception {
    HdfsTestUtil.teardownClass(dfsCluster);
    System.clearProperty("solr.hdfs.home");
    System.clearProperty("solr.authorization.sentry.site");
    dfsCluster = null;
    extraRequestFilters = null;
  }

  @Override
  protected String getDataDir(String dataDir) throws IOException {
    return HdfsTestUtil.getDataDir(dfsCluster, dataDir);
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    System.setProperty("numShards", Integer.toString(sliceCount));
    System.setProperty("solr.xml.persist", "true");
  }

  @Override
  protected String getSolrXml() {
    return "solr-no-core.xml";
  }

  @Override
  protected String getCloudSolrConfig() {
    return "solrconfig.xml";
  }

  @Override
  public SortedMap<Class,String> getExtraRequestFilters() {
    return extraRequestFilters;
  }

  /**
   * Subclasses can override this to change a test's solr home
   * (default is in test-files)
   */
  public String getSolrHome() {
    return SolrTestCaseJ4.TEST_HOME();
  }

  protected void uploadConfigDirToZk(String collectionConfigDir) throws Exception {
    SolrDispatchFilter dispatchFilter =
      (SolrDispatchFilter) jettys.get(0).getDispatchFilter().getFilter();
    ZkController zkController = dispatchFilter.getCores().getZkController();
    // conf1 is the config used by AbstractFullDistribZkTestBase
    zkController.uploadConfigDir(new File(collectionConfigDir), "conf1");
  }
}
