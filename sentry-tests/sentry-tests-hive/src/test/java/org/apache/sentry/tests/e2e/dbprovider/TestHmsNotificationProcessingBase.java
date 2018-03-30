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

package org.apache.sentry.tests.e2e.dbprovider;

import org.apache.sentry.tests.e2e.hdfs.TestHDFSIntegrationBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestHmsNotificationProcessingBase  extends TestHDFSIntegrationBase{
  static final Logger LOGGER = LoggerFactory.getLogger(TestHmsNotificationProcessingBase.class);
  protected final static int SHOW_GRANT_TABLE_POSITION = 2;
  protected final static int SHOW_GRANT_DB_POSITION = 1;
  protected static final String DB1 = "db_1",
          DB2 = "db_2",
          tableName1 = "tb_1",
          tableName2 = "tb_2";

  // verify all the test privileges are dropped as we drop the objects
  protected void verifyPrivilegesDropped(Statement statement)
          throws Exception {
    verifyDbPrivilegesDropped(statement);
    verifyTablePrivilegesDropped(statement);
  }

  // verify all the test privileges are dropped as we drop the objects
  protected void verifyTablePrivilegesDropped(Statement statement)
          throws Exception {
    List<String> roles = getRoles(statement);
    verifyIfAllPrivilegeAreDropped(statement, roles, tableName1,
            SHOW_GRANT_TABLE_POSITION);
  }

  // verify all the test privileges are dropped as we drop the objects
  protected void verifyDbPrivilegesDropped(Statement statement) throws Exception {
    List<String> roles = getRoles(statement);
    verifyIfAllPrivilegeAreDropped(statement, roles, DB2, SHOW_GRANT_DB_POSITION);
    verifyIfAllPrivilegeAreDropped(statement, roles, DB1, SHOW_GRANT_DB_POSITION);

  }

  // verify all the test privileges are not dropped as we drop the objects
  protected void verifyPrivilegesCount(Statement statement, int count)
          throws Exception {
    int privilegeCount = 0;
    List<String> roles = getRoles(statement);
    for (String roleName : roles) {
      if(roleName.compareTo("admin_role") == 0) {
        continue;
      }
      ResultSet resultSet = statement.executeQuery("SHOW GRANT ROLE "
              + roleName);
      while (resultSet.next()) {
        privilegeCount++;
      }
      resultSet.close();
    }
    assertEquals("Privilege count do not match", count, privilegeCount);
  }
}
