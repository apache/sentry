package org.apache.sentry.tests.e2e.hive.fs;

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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.PrivilegedExceptionAction;

public class S3DFS extends AbstractDFS {
  private static final Logger LOGGER = LoggerFactory
      .getLogger(S3DFS.class);

  private UserGroupInformation ugi;

  S3DFS() throws Exception{
    ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(TEST_USER, KEYTAB_LOCATION + "/" + TEST_USER + ".keytab");
    fileSystem = getFS(ugi);
    LOGGER.info("fileSystem URI = " + fileSystem.getUri());
    LOGGER.info("Kinited as testUser = " + TEST_USER);
  }

  private FileSystem getFS(UserGroupInformation ugi) throws Exception {
    return ugi.doAs(new PrivilegedExceptionAction<FileSystem>() {
      public FileSystem run() throws Exception {
        Configuration conf = new Configuration();
        return FileSystem.get(conf);
      }
    });
  }
}
