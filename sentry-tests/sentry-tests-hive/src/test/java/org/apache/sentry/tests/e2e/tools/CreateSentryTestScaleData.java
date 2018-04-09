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
package org.apache.sentry.tests.e2e.tools;

import com.google.common.base.Strings;
import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.sentry.tests.e2e.hive.hiveserver.UnmanagedHiveServer;

import java.io.File;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * This code attempts to create Sentry and HMS synthetic test data.
 * Before run:
 *  export HIVE_CONF_DIR=/etc/hive/conf
 *  export HIVE_LIB=/usr/lib/hive
 *  export HADOOP_CONF_DIR=/etc/hadoop/conf
 *  export HADOOP_CLASSPATH=${HIVE_LIB}/lib/*:${HADOOP_CLASSPATH}
 *  export HADOOP_CLASSPATH=${HIVE_CONF_DIR}/*:${HADOOP_CLASSPATH}
 *  export HADOOP_CLASSPATH=${HADOOP_CONF_DIR}/*:${HADOOP_CLASSPATH}
 *  export HADOOP_OPTS="$HADOOP_OPTS -Dhive.server2.thrift.bind.host=hostname
 *    -Dsentry.e2e.hive.keytabs.location=/keytabs
 *    -Dsentry.scale.test.config.path=/tmp/conf"
 * To run it:
 *  hadoop jar test-tools.jar --scale
 */
public class CreateSentryTestScaleData {
  // This class stores thread test results
  public class TestDataStats {
    long num_databases;
    long num_tables;
    long num_views;
    long num_partitions;
    long num_columns;
    int num_uris;
    public void addCounts(TestDataStats testDataStats) {
      this.num_databases += testDataStats.num_databases;
      this.num_tables += testDataStats.num_tables;
      this.num_views += testDataStats.num_views;
      this.num_partitions += testDataStats.num_partitions;
      this.num_columns += testDataStats.num_columns;
      this.num_uris += testDataStats.num_uris;
    }
  }

  public class TestStatus {
    TestDataStats testDataStats = new TestDataStats(); // store object counts
    TestDataStats privilegeStatus = new TestDataStats(); // store object's privilege counts
    int failed = 0;
    long elapsed_time = 0L;
    @Override
    public String toString() {
      String objects = String.format("total databases(%d); tables(%d), views(%d), partitions(%d), columns(%d)",
          total_num_databases.get(), testDataStats.num_tables, testDataStats.num_views,
          testDataStats.num_partitions, testDataStats.num_columns);
      String privileges = String.format("database privileges(%d), table privileges(%d), view privileges(%d), " +
              "partition privileges(%d), column privileges(%d), uri privileges(%d)", privilegeStatus.num_databases,
          privilegeStatus.num_tables, privilegeStatus.num_views, privilegeStatus.num_partitions,
          privilegeStatus.num_columns, privilegeStatus.num_uris);
      return String.format("Objects status: %s;\nPrivileges status: %s; Total roles(%d) and groups(%d);\nFailed threads(%d), running time(%d secs).",
          objects, privileges, NUM_OF_ROLES, NUM_OF_GROUPS, failed, elapsed_time);
    }
  }

  final static String CONFIG_FILE_NAME = "sentry_scale_test_config.xml";
  final static String CONFIG_PATH = System.getProperty("sentry.scale.test.config.path");
  private static Configuration scaleConfig = new Configuration();
  static {
    StringBuilder fullPath = new StringBuilder();
    if (CONFIG_PATH != null && CONFIG_PATH.length() > 0 ) {
      fullPath.append(CONFIG_PATH);
    }
    if (fullPath.length() > 0 && fullPath.lastIndexOf("/") != fullPath.length() - 1) {
      fullPath.append("/");
    }
    fullPath.append(CONFIG_FILE_NAME);
    URL url = null;
    try {
      url = new File(fullPath.toString()).toURI().toURL();
      System.out.println("Reading config file from url: " + url.toString());
      scaleConfig.addResource(url, true);
    } catch (Exception ex) {
      System.err.println("Failed to load config file from local file system: " + url.toString());
      throw new RuntimeException(ex);
    }
  };

  final static int NUM_OF_THREADS_TO_CREATE_DATA = scaleConfig.getInt("sentry.scale.test.threads", 1);
  final static int NUM_OF_DATABASES = scaleConfig.getInt("sentry.scale.test.num.databases", 4);
  final static int MAX_TABLES_PER_DATABASE = scaleConfig.getInt("sentry.scale.test.max.tables.per.database", 4);
  final static int MED_TABLES_PER_DATABASE = scaleConfig.getInt("sentry.scale.test.med.tables.per.database", 2) ;
  final static int AVG_VIEWS_PER_DATABASE = scaleConfig.getInt("sentry.scale.test.avg.views.per.database", 1);
  final static int MAX_PARTITIONS_PER_TABLE = scaleConfig.getInt("sentry.scale.test.max.partitions.per.table", 2);
  final static int MED_PARTITIONS_PER_TABLE = scaleConfig.getInt("sentry.scale.test.med.partitions.per.table", 1);
  final static int MAX_COLUMNS_PER_TABLE = scaleConfig.getInt("sentry.scale.test.max.columns.per.table", 2);
  final static int MED_COLUMNS_PER_TABLE = scaleConfig.getInt("sentry.scale.test.med.columns.per.table", 1);
  final static int EXTERNAL_VS_MANAGED_TBLS = scaleConfig.getInt("sentry.scale.test.external.vs.managed.tables", 2);

  final static int NUM_OF_ROLES = scaleConfig.getInt("sentry.scale.test.num.roles", 10);
  final static int NUM_OF_GROUPS = scaleConfig.getInt("sentry.scale.test.num.groups", 5);
  final static int MAX_ROLES_PER_GROUP = scaleConfig.getInt("sentry.scale.test.max.roles.per.group", 5);
  final static int DATABASE_PRIVILEGES_PER_THREAD = scaleConfig.getInt("sentry.scale.test.database.privileges.per.thread", 2);
  final static int TABLE_PRIVILEGES_PER_THREAD = scaleConfig.getInt("sentry.scale.test.table.privileges.per.thread", 2);
  final static int VIEW_PRIVILEGES_PER_THREAD = scaleConfig.getInt("sentry.scale.test.view.privileges.per.thread", 1);
  final static int URI_PRIVILEGES_PER_THREAD = scaleConfig.getInt("sentry.scale.test.uri.privileges.per.thread", 1);
  final static int COLUMN_PRIVILEGES_PER_THREAD = scaleConfig.getInt("sentry.scale.test.column.privileges.per.thread", 1);

  final static String ADMIN = scaleConfig.get("sentry.e2etest.admin.group", "hive");
  final static String KEYTAB_LOCATION = scaleConfig.get("sentry.e2e.hive.keytabs.location");
  final static boolean TEST_DEBUG = scaleConfig.getBoolean("sentry.e2etest.scale.data.debug", false);
  final static String EXT_TEST_DATA_PATH = scaleConfig.get("sentry.e2etest.scale.data.dir", "extdata");
  final static long TOOL_MAX_RUNNING_SECS = scaleConfig.getInt("sentry.tests.e2e.tools.scale.max.running.seconds", 300);
  final static long THREAD_WAITING_TIME_SECS = scaleConfig.getInt("sentry.tests.e2e.tools.scale.thread.waiting.seconds", 5);

  final String[] COLUMN = {"s", "STRING"};
  final static String TEST_DATA_PREFIX = "scale_test";
  private static FileSystem fileSystem = null;
  private static UnmanagedHiveServer hiveServer = null;
  private static final String HIVE_DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";
  static {
    try {
      Class.forName(HIVE_DRIVER_NAME);
      hiveServer = new UnmanagedHiveServer();
      UserGroupInformation ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI("hdfs", getKeytabPath("hdfs"));
      fileSystem = ugi.doAs(new PrivilegedExceptionAction<FileSystem>() {
        @Override
        public FileSystem run() throws Exception {
          Configuration conf = new Configuration();
          String defaultFs = hiveServer.getProperty("fs.defaultFS");
          if (Strings.isNullOrEmpty(defaultFs)) {
            defaultFs = scaleConfig.get("fs.defaultFS");
          }
          conf.set("fs.defaultFS", defaultFs);
          FileSystem fileSystem = FileSystem.get(conf);
          Preconditions.checkNotNull(fileSystem);
          return fileSystem;
        }
      });
      Path extPathRoot = new Path(String.format("/%s", EXT_TEST_DATA_PATH));
      System.out.println("Creating external data root dir:" + extPathRoot.toString());
      if (fileSystem.exists(extPathRoot)) {
        fileSystem.delete(extPathRoot, true);
      }
      fileSystem.mkdirs(extPathRoot, new FsPermission((short) 0777));
      fileSystem.setOwner(extPathRoot, ADMIN, ADMIN);
    } catch (Exception ex) {
      throw new RuntimeException("Failed to create FileSystem: ", ex);
    }
  }

  private AtomicInteger total_num_databases = new AtomicInteger(0);

  //private functions start from here
  private static String getKeytabPath(String runAsUser) {
    if (KEYTAB_LOCATION != null && KEYTAB_LOCATION.length() > 0) {
      return KEYTAB_LOCATION + "/" + runAsUser + ".keytab";
    }
    return runAsUser + ".keytab"; //in classpath
  }

  private void print(String msg, String level) {
    switch (level) {
      case "ERROR":
        System.err.println(String.format("[%s]  [ERROR] %s", Thread.currentThread().getName(), msg));
        break;
      case "DEBUG":
        if (TEST_DEBUG) {
          System.out.println(String.format("[%s]  [DEBUG] %s", Thread.currentThread().getName(), msg));
        }
        break;
      default:
        System.out.println(String.format("[%s]  [%s]  %s", Thread.currentThread().getName(), level, msg));
        break;
    }
  }

  private int getNumOfObjectsPerDb(int dbSeq, int tbSeq, String objType, TestDataStats testDataStats) {
    int num = 0;
    Random rand = new Random();
    switch (objType) {
      case "TABLE":
        num = dbSeq == 0 ? MAX_TABLES_PER_DATABASE : rand.nextInt(MED_TABLES_PER_DATABASE) + 1;
        testDataStats.num_tables += num;
        break;
      case "COLUMN":
        num = (dbSeq == 1 && (tbSeq == 0 || tbSeq == 1)) ? MAX_COLUMNS_PER_TABLE : rand.nextInt(MED_COLUMNS_PER_TABLE) + 1;
        testDataStats.num_columns += num;
        break;
      case "PARTITION":
        num = (dbSeq == 2 && (tbSeq == 0 || tbSeq == 1)) ? MAX_PARTITIONS_PER_TABLE : rand.nextInt(MED_PARTITIONS_PER_TABLE) + 1;
        testDataStats.num_partitions += num;
        break;
      default:
        break;
    }
    return num;
  }

  private String createManagedTableCmd(String tblName, int numOfCols) {
    StringBuilder command = new StringBuilder("CREATE TABLE IF NOT EXISTS ");
    command.append(tblName + "(");
    for(int i = 0; i < numOfCols; i++) {
      command.append(COLUMN[0]);
      command.append(i);
      command.append(" " + COLUMN[1]);
      if (i < numOfCols - 1) {
        command.append(", ");
      }
    }
    command.append(")");
    return command.toString();
  }

  private void createExternalTable(String tblName, int numOfPars, String extPath,
                                        Statement statement) throws Exception {
    exec(statement, String.format("CREATE EXTERNAL TABLE IF NOT EXISTS %s (num INT) PARTITIONED BY (%s %s) LOCATION '%s'",
        tblName, COLUMN[0], COLUMN[1], extPath));

    for(int i = 0; i < numOfPars; i ++) {
      String strParPath = String.format("%s/%d", extPath, i);
      createExtTablePath(strParPath);
      exec(statement, String.format("ALTER TABLE %s ADD PARTITION (%s='%d') LOCATION '%s'",
          tblName, COLUMN[0], i, strParPath));
    }
  }

  private String createExtTablePath(String strPath) throws IOException {
    String strFullPath = String.format("/%s/%s", EXT_TEST_DATA_PATH, strPath);
    Path extPath = new Path(strFullPath);
    if (fileSystem.exists(extPath)) {
      fileSystem.delete(extPath, true);
    }
    if (fileSystem.mkdirs(extPath, new FsPermission((short) 0777))) {
      fileSystem.setOwner(extPath, ADMIN, ADMIN);
    } else {
      throw new IOException("mkdir failed to create " + strFullPath);
    }
    return strFullPath;
  }

  private void exec(Statement statement, String cmd) throws SQLException {
    print("Executing [" + cmd + "]", "DEBUG");
    statement.execute(cmd);
  }

  private String getRoleName() {
    return getRoleName(new Random().nextInt(NUM_OF_ROLES));
  }

  private String getRoleName(final int seq) {
    return String.format("%s_role_%d", TEST_DATA_PREFIX, seq);
  }

  private String getGroupName() {
    return getGroupName(new Random().nextInt(NUM_OF_GROUPS));
  }

  private String getGroupName(final int seq) {
    return String.format("%s_group_%d", TEST_DATA_PREFIX, seq);
  }

  private void grantPrivileges(Statement statement, String objName, String objType,
                               TestDataStats privilegeStatus) throws SQLException {
    String roleName = getRoleName();
    switch (objType) {
      case "DATABASE":
        if (privilegeStatus.num_databases < DATABASE_PRIVILEGES_PER_THREAD) {
          exec(statement, String.format("GRANT %s ON DATABASE %s TO ROLE %s",
              (new Random().nextBoolean() ? "SELECT" : "INSERT"), objName, roleName));
          privilegeStatus.num_databases++;
        }
        break;
      case "TABLE":
        if (privilegeStatus.num_tables < TABLE_PRIVILEGES_PER_THREAD) {
          exec(statement, String.format("GRANT %s ON TABLE %s TO ROLE %s",
              (new Random().nextBoolean() ? "SELECT" : "INSERT"), objName, roleName));
          privilegeStatus.num_tables++;
        }
        break;
      case "VIEW":
        if (privilegeStatus.num_views < VIEW_PRIVILEGES_PER_THREAD) {
          exec(statement, String.format("GRANT %s ON TABLE %s TO ROLE %s",
              (new Random().nextBoolean() ? "SELECT" : "INSERT"), objName, roleName));
          privilegeStatus.num_views++;
        }
        break;
      case "COLUMN":
        if (privilegeStatus.num_columns < COLUMN_PRIVILEGES_PER_THREAD) {
          exec(statement, String.format("GRANT SELECT(%s1) ON TABLE %s TO ROLE %s", COLUMN[0], objName, roleName));
          privilegeStatus.num_columns++;
        }
        break;
      case "PARTITION":
        if (privilegeStatus.num_partitions < COLUMN_PRIVILEGES_PER_THREAD) {
          exec(statement, String.format("GRANT SELECT(num) ON TABLE %s TO ROLE %s", objName, roleName));
          privilegeStatus.num_partitions++;
        }
        break;
      case "URI":
        if (privilegeStatus.num_uris < URI_PRIVILEGES_PER_THREAD) {
          exec(statement, String.format("GRANT ALL ON URI '%s' TO ROLE %s", objName, roleName));
          privilegeStatus.num_uris++;
        }
        break;
      case "MAX_PAR":
        exec(statement, String.format("GRANT SELECT(num) ON TABLE %s TO ROLE %s", objName, roleName));
        privilegeStatus.num_partitions += 1;
      case "MAX_COL":
        StringBuilder grantPars = new StringBuilder("GRANT SELECT(");
        for(int i = 0; i <  MAX_COLUMNS_PER_TABLE - 1; i++) {
          grantPars.append(String.format("%s%d, ", COLUMN[0], i));
        }
        grantPars.append(String.format("%s%d", COLUMN[0], MAX_COLUMNS_PER_TABLE - 1));
        grantPars.append(String.format(") ON TABLE %s TO ROLE %s", objName, roleName));
        exec(statement, grantPars.toString());
        break;
      default:
        break;
    }
  }

  //create access controlled objects and their privileges
  private void createTestData(TestStatus testStatus) throws Exception {
    long startTime = System.currentTimeMillis();
    try (Connection con = hiveServer.createConnection(ADMIN, ADMIN)) {
      try (Statement statement = con.createStatement()) {
        while (total_num_databases.get() < NUM_OF_DATABASES) {
          TestDataStats dbTestDataStats = new TestDataStats();
          TestDataStats dbPrivilegeStatus = new TestDataStats();
          int dbSeq = total_num_databases.getAndIncrement();
          String testDb = String.format("%s_db_%d", TEST_DATA_PREFIX, dbSeq);
          print("Creating database " + testDb + " and its objects and privileges.", "INFO");
          exec(statement, "CREATE DATABASE IF NOT EXISTS " + testDb);
          exec(statement, "USE " + testDb);
          grantPrivileges(statement, testDb, "DATABASE", dbPrivilegeStatus);
          int num_of_tables = getNumOfObjectsPerDb(dbSeq, 0, "TABLE", dbTestDataStats);
          for (int tb = 0; tb < num_of_tables; tb ++) {
            String testView = "", extPath = "";
            String testTbl = String.format("%s_tbl_%d", testDb, tb);
            //external table
            if (tb % EXTERNAL_VS_MANAGED_TBLS != 0) {
              print("Creating external table " + testTbl + " and its privileges in database " + testDb, "INFO");
              int num_of_pars = getNumOfObjectsPerDb(dbSeq, tb, "PARTITION", dbTestDataStats);
              extPath = String.format("/%s/%s/%s", EXT_TEST_DATA_PATH, testDb, testTbl);
              createExtTablePath(extPath);
              createExternalTable(testTbl, num_of_pars, extPath, statement);
              grantPrivileges(statement, testTbl, "TABLE", dbPrivilegeStatus);
              if (num_of_pars == MAX_PARTITIONS_PER_TABLE) {
                grantPrivileges(statement, testTbl, "MAX_PAR", dbPrivilegeStatus);
              } else {
                grantPrivileges(statement, testTbl, "PARTITION", dbPrivilegeStatus);
              }
              grantPrivileges(statement, extPath, "URI", dbPrivilegeStatus);
            } else { //managed table
              int num_of_columns = getNumOfObjectsPerDb(dbSeq, tb, "COLUMN", dbTestDataStats);
              exec(statement, createManagedTableCmd(testTbl, num_of_columns));
              grantPrivileges(statement, testTbl, "TABLE", dbPrivilegeStatus);
              if (num_of_columns == MAX_COLUMNS_PER_TABLE) {
                grantPrivileges(statement, testTbl, "MAX_COL", dbPrivilegeStatus);
              } else {
                grantPrivileges(statement, testTbl, "COLUMN", dbPrivilegeStatus);
              }
            }
            //view
            if (dbTestDataStats.num_views < AVG_VIEWS_PER_DATABASE) {
              testView = String.format("%s_view", testTbl);
              exec(statement, "CREATE VIEW " + testView + " AS SELECT * FROM " + testTbl);
              dbTestDataStats.num_views++;
              grantPrivileges(statement, testView, "VIEW", dbPrivilegeStatus);
            }
          }
          testStatus.testDataStats.addCounts(dbTestDataStats);
          testStatus.privilegeStatus.addCounts(dbPrivilegeStatus);
          testStatus.elapsed_time = (System.currentTimeMillis() - startTime) / 1000L;
        }
      }
    }
    print("Thread done with creating data: " + testStatus.toString(), "INFO");
  }

  /**
   * Attempt to create scale data in HMS and Sentry
   * @return
   * @throws Exception
   */
  public TestStatus create() throws Exception {
    long createStartTime = System.currentTimeMillis();
    TestStatus testStatus = new TestStatus();
    try (Connection con = hiveServer.createConnection(ADMIN, ADMIN)) {
      try (Statement statement = con.createStatement()) {
        //1. create roles
        print("Creating " + NUM_OF_ROLES + " roles.", "INFO");
        for(int i = 0; i < NUM_OF_ROLES; i++) {
          exec(statement, "CREATE ROLE " + getRoleName(i));
        }
        //2. map roles to groups
        print("Assigning " + NUM_OF_ROLES + " roles to " + NUM_OF_GROUPS + " groups.", "INFO");
        String largestGroup = getGroupName();
        for(int i = 0; i < MAX_ROLES_PER_GROUP; i++) {
          exec(statement, String.format("GRANT ROLE %s TO GROUP %s", getRoleName(i), largestGroup));
        }
        for(int i = 0; i < NUM_OF_ROLES; i++) {
          exec(statement, String.format("GRANT ROLE %s TO GROUP %s", getRoleName(i), getGroupName()));
        }
      }
    }
    //3. create HMS objects and privileges
    ExecutorService pool = Executors.newFixedThreadPool(NUM_OF_THREADS_TO_CREATE_DATA);
    List<Future<TestStatus>> jobs = new ArrayList<>();
    for (int i = 0; i < NUM_OF_THREADS_TO_CREATE_DATA; i++) {
      Future<TestStatus> job = pool.submit(new Callable<TestStatus>() {
        @Override
        public TestStatus call() {
          TestStatus threadTestStatus = new TestStatus();
          try {
            createTestData(threadTestStatus);
          } catch (Exception ex) {
            threadTestStatus.failed += 1;
            print("create() throws exception: " + ex + ", Stacktrace: " + Arrays.deepToString(ex.getStackTrace()), "ERROR");
          }
          return threadTestStatus;
        }
      });
      jobs.add(job);
    }
    print("Submitted " + jobs.size() + " jobs", "INFO");
    for(Future<TestStatus> job : jobs) {
      try {
        long jobStartTime = System.currentTimeMillis(), jobElapsedTimeInSecs = 0L;
        while (!job.isDone() && jobElapsedTimeInSecs < TOOL_MAX_RUNNING_SECS) {
          Thread.sleep(THREAD_WAITING_TIME_SECS * 1000); //millis
          jobElapsedTimeInSecs = (System.currentTimeMillis() - jobStartTime) / 1000L;
          print("Waiting for all threads to finish, elapsed time = " + jobElapsedTimeInSecs + " seconds.", "INFO");
        }
        if (!job.isDone()) {
          testStatus.failed += 1;
          print("Longest waiting time has passed. Thread fails to return.", "ERROR");
        } else {
          TestStatus threadTestStatus = job.get();
          if (threadTestStatus != null) {
            testStatus.testDataStats.addCounts(threadTestStatus.testDataStats);
            testStatus.privilegeStatus.addCounts(threadTestStatus.privilegeStatus);
            testStatus.failed += threadTestStatus.failed;
          } else {
            print("Thread returns null status.", "ERROR");
            testStatus.failed += 1;
          }
        }
      } catch (Exception ex) {
        print("Thread job throws exception: " + ex, "ERROR");
        testStatus.failed += 1;
      }
    }
    pool.shutdown();
    try {
      if (!pool.awaitTermination(1, TimeUnit.MINUTES)) {
        pool.shutdownNow();
      }
    } catch (InterruptedException ex) {
      print("Failed to shut down pool: " + ex, "ERROR");
    }
    testStatus.elapsed_time = (System.currentTimeMillis() - createStartTime) / 1000L; //secs
    return testStatus;
  }

  /**
   * Clean up created scale data
   * @throws Exception
   */
  public void cleanUpScaleData() throws Exception {
    try (Connection con = hiveServer.createConnection(ADMIN, ADMIN)) {
      try (Statement statement = con.createStatement()) {
        ResultSet resultSet = statement.executeQuery("SHOW DATABASES");
        while (resultSet.next()) {
          String dbName = resultSet.getString(1);
          if (!dbName.startsWith(TEST_DATA_PREFIX)) {
            continue;
          }
          try (Statement statement1 = con.createStatement()) {
            exec(statement1, "DROP DATABASE " + dbName + " CASCADE");
          } catch (Exception ex) {
            print("Fails to clean up DATABASE " + dbName + ": " + ex, "ERROR");
          }
        }
        if (resultSet != null) {
          resultSet.close();
        }
        resultSet = statement.executeQuery("SHOW ROLES");
        while (resultSet.next()) {
          String roleName = resultSet.getString(1);
          if (!roleName.startsWith(TEST_DATA_PREFIX)) {
            continue;
          }
          try (Statement statement1 = con.createStatement()) {
            exec(statement1, "DROP ROLE " + roleName);
          } catch (Exception ex) {
            print("Fails to clean up ROLE " + roleName + ": " + ex, "ERROR");
          }
        }
        if (resultSet != null) {
          resultSet.close();
        }
      }
    }
  }
}
