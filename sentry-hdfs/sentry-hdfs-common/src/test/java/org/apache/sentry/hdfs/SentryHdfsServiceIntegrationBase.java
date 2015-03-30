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

import java.security.PrivilegedExceptionAction;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.sentry.SentryUserException;
import org.apache.sentry.hdfs.ServiceConstants.ClientConfig;
import org.apache.sentry.service.thrift.SentryServiceIntegrationBase;
import org.junit.After;
import org.junit.Before;

public class SentryHdfsServiceIntegrationBase extends
    SentryServiceIntegrationBase {

  protected SentryHDFSServiceClient hdfsClient;

  @Before
  public void before() throws Exception {
    conf.set("hadoop.security.authentication", "kerberos");
    UserGroupInformation.setConfiguration(conf);
    UserGroupInformation.loginUserFromKeytab(CLIENT_PRINCIPAL,
        clientKeytab.getPath());

    connectToHdfsSyncService();
  }

  @After
  public void after() throws SentryUserException {
    if (hdfsClient != null) {
      hdfsClient.close();
    }
  }

  protected void connectToHdfsSyncService() throws Exception {
    if (hdfsClient != null) {
      hdfsClient.close();
    }

    // SentryHdfs client configuration setup
    conf.set(ClientConfig.SERVER_RPC_ADDRESS, server.getAddress()
        .getHostName());
    conf.set(ClientConfig.SERVER_RPC_ADDRESS, server.getAddress()
        .getHostName());
    conf.set(ClientConfig.SERVER_RPC_PORT,
        String.valueOf(server.getAddress().getPort()));

    if (kerberos) {
      conf.set(ClientConfig.SECURITY_MODE, ClientConfig.SECURITY_MODE_KERBEROS);
      conf.set(ClientConfig.SECURITY_USE_UGI_TRANSPORT, "true");
      conf.set(ClientConfig.PRINCIPAL, getServerKerberosName());
      hdfsClient = UserGroupInformation.getLoginUser().doAs(
          new PrivilegedExceptionAction<SentryHDFSServiceClient>() {
            @Override
            public SentryHDFSServiceClient run() throws Exception {
              return new SentryHDFSServiceClient(conf);
            }
          });
    } else {
      hdfsClient = new SentryHDFSServiceClient(conf);
    }
    hdfsClient.close();
  }

}
