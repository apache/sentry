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
import java.net.MalformedURLException;
import java.util.Comparator;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.sentry.binding.solr.HdfsTestUtil;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.servlet.SolrDispatchFilter;
import org.apache.solr.util.BadHdfsThreadsFilter;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import com.google.common.io.Files;

/**
 * This is a base class for all unit tests related to Solr Sentry file-provider.
 *
 */
@ThreadLeakFilters(defaultFilters = true, filters = {
    BadHdfsThreadsFilter.class // hdfs currently leaks thread(s)
})
@SolrTestCaseJ4.SuppressSSL
public class AbstractSolrSentryTestWithFileProvider extends AbstractSolrSentryTestBase {
  protected static SortedMap<Class, String> extraRequestFilters;
  protected static MiniDFSCluster dfsCluster;
  protected static MiniSolrCloudCluster miniSolrCloudCluster;

  @BeforeClass
  public static void beforeTestSimpleSolrEndToEnd() throws Exception {
    dfsCluster = HdfsTestUtil.setupClass(new File(Files.createTempDir(),
      AbstractSolrSentryTestBase.class.getName() + "_"
        + System.currentTimeMillis()).getAbsolutePath());
    File sentrySite = setupSentry(dfsCluster);
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
    extraRequestFilters.put(ModifiableUserAuthenticationFilter.class, "*");
    File solrXml = new File(RESOURCES_DIR, "solr-no-core.xml");
    miniSolrCloudCluster = new MiniSolrCloudCluster(NUM_SERVERS, null, solrXml,
      null, extraRequestFilters);
  }

  @AfterClass
  public static void teardownClass() throws Exception {
    HdfsTestUtil.teardownClass(dfsCluster);
    System.clearProperty("solr.hdfs.home");
    System.clearProperty("solr.authorization.sentry.site");
    dfsCluster = null;
    extraRequestFilters = null;
    if (miniSolrCloudCluster != null) {
      miniSolrCloudCluster.shutdown();
      miniSolrCloudCluster = null;
    }
  }

  protected CloudSolrServer getCloudSolrServer(String collectionName) throws MalformedURLException {
    CloudSolrServer cloudSolrServer = new CloudSolrServer(miniSolrCloudCluster.getZkServer().getZkAddress(),
        RANDOM.nextBoolean());
    cloudSolrServer.setDefaultCollection(collectionName);
    cloudSolrServer.connect();
    return cloudSolrServer;
  }

  protected CloudSolrServer createNewCloudSolrServer() throws Exception {
    CloudSolrServer css = new CloudSolrServer(miniSolrCloudCluster.getZkServer().getZkAddress());
    css.connect();
    return css;
  }

  protected ZkController getZkController() {
    SolrDispatchFilter dispatchFilter =
      (SolrDispatchFilter) miniSolrCloudCluster.getJettySolrRunners().get(0).getDispatchFilter().getFilter();
    return dispatchFilter.getCores().getZkController();
  }
}
