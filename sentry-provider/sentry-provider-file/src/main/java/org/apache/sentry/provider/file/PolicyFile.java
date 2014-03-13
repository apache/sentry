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

package org.apache.sentry.provider.file;

import static org.apache.sentry.provider.file.PolicyFileConstants.DATABASES;
import static org.apache.sentry.provider.file.PolicyFileConstants.GROUPS;
import static org.apache.sentry.provider.file.PolicyFileConstants.ROLES;
import static org.apache.sentry.provider.file.PolicyFileConstants.USERS;

import java.io.File;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.io.Files;

/**
 * PolicyFile creator. Written specifically to be used with tests. Specifically
 * due to the fact that methods that would typically return true or false to
 * indicate success or failure these methods throw an unchecked exception.
 * This is because in a test if you mean to remove a user from the policy file,
 * the user should absolutely be there. If not, the test is mis-behaving.
 */
@VisibleForTesting
public class PolicyFile {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(PolicyFile.class);

  private static final String NL = System.getProperty("line.separator", "\n");

  private final Map<String, String> databasesToPolicyFiles = Maps.newHashMap();
  private final Multimap<String, String> usersToGroups = ArrayListMultimap.create();
  private final Multimap<String, String> groupsToRoles = ArrayListMultimap.create();
  private final Multimap<String, String> rolesToPermissions = ArrayListMultimap.create();

  public PolicyFile addRolesToGroup(String groupName, String... roleNames) {
    return addRolesToGroup(groupName, false, roleNames);
  }
  public PolicyFile addRolesToGroup(String groupName, boolean allowDuplicates, String... roleNames) {
    return add(groupsToRoles.get(groupName), allowDuplicates, roleNames);
  }
  public PolicyFile addPermissionsToRole(String roleName, String... permissionNames) {
    return addPermissionsToRole(roleName, false, permissionNames);
  }
  public PolicyFile addPermissionsToRole(String roleName, boolean allowDuplicates, String... permissionNames) {
    return add(rolesToPermissions.get(roleName), allowDuplicates, permissionNames);
  }
  public PolicyFile addGroupsToUser(String userName, String... groupNames) {
    LOGGER.warn("Static user:group mapping is not being used");
    return addGroupsToUser(userName, false, groupNames);
  }
  public PolicyFile addGroupsToUser(String userName, boolean allowDuplicates, String... groupNames) {
    LOGGER.warn("Static user:group mapping is not being used");
    return add(usersToGroups.get(userName), allowDuplicates, groupNames);
  }
  public PolicyFile setUserGroupMapping(Map<String, String> mapping){
    for(String key: mapping.keySet()){
      usersToGroups.put(key, mapping.get(key));
    }
    return this;
  }
  public PolicyFile addDatabase(String databaseName, String path) {
    String oldPath;
    if((oldPath = databasesToPolicyFiles.put(databaseName, path)) != null) {
      throw new IllegalStateException("Database " + databaseName + " already existed in " +
          databasesToPolicyFiles + " with value of " + oldPath);
    }
    databasesToPolicyFiles.put(databaseName, path);
    return this;
  }
  public PolicyFile removeRolesFromGroup(String groupName, String... roleNames) {
    return remove(groupsToRoles.get(groupName), roleNames);
  }
  public PolicyFile removePermissionsFromRole(String roleName, String... permissionNames) {
    return remove(rolesToPermissions.get(roleName), permissionNames);
  }
  public PolicyFile removeGroupsFromUser(String userName, String... groupNames) {
    LOGGER.warn("Static user:group mapping is not being used");
    return remove(usersToGroups.get(userName), groupNames);
  }
  public PolicyFile removeDatabase(String databaseName) {
    if(databasesToPolicyFiles.remove(databaseName) == null) {
      throw new IllegalStateException("Database " + databaseName + " did not exist in " +
          databasesToPolicyFiles);
    }
    return this;
  }
  public PolicyFile copy() {
    PolicyFile other = new PolicyFile();
    other.databasesToPolicyFiles.putAll(databasesToPolicyFiles);
    other.usersToGroups.putAll(usersToGroups);
    other.groupsToRoles.putAll(groupsToRoles);
    other.rolesToPermissions.putAll(rolesToPermissions);
    return other;
  }

  public void write(File file) throws Exception {
    if(file.exists() && !file.delete()) {
      throw new IllegalStateException("Unable to delete " + file);
    }
    String contents = Joiner.on(NL)
        .join(getSection(DATABASES, databasesToPolicyFiles),
            getSection(USERS, usersToGroups),
            getSection(GROUPS, groupsToRoles),
            getSection(ROLES, rolesToPermissions),
            "");
    LOGGER.info("Writing policy file to " + file + ":\n" + contents);
    Files.write(contents, file, Charsets.UTF_8);

    String hiveServer2 = System.getProperty("sentry.e2etest.hiveServer2Type", "InternalHiveServer2");

    //Currently policyOnHDFS is only supported for UnmanagedHiveServer, and global policy file is required to be on hdfs
    if(hiveServer2.equals("UnmanagedHiveServer2")) {
      String policyOnHDFS = System.getProperty("sentry.e2etest.hive.policyOnHDFS", "true");
      if(policyOnHDFS.trim().equalsIgnoreCase("true") || file.getName().equalsIgnoreCase("sentry-provider.ini")){
        String policyLocation = System.getProperty("sentry.e2etest.hive.policy.location", "/user/hive/sentry");
        String policyFileLocation = policyLocation + "/" + file.getName();
        LOGGER.info("Moving policy file to " + policyFileLocation);
        String userKeytab = System.getProperty("sentry.e2etest.hive.policyOwnerKeytab");
        String userPrincipal = System.getProperty("sentry.e2etest.hive.policyOwnerPrincipal");
        Preconditions.checkNotNull(userKeytab);
        Preconditions.checkNotNull(userPrincipal);
        hdfsPut(file, policyFileLocation, userKeytab, userPrincipal);
      }
    }
  }

  private void hdfsPut(File file, String hdfsPath, String userKeytab, String userPrincipal) throws Exception {
    String command, status;
    Process p;

    command = "kinit -kt " + userKeytab +  " " + userPrincipal;
    p = Runtime.getRuntime().exec(command);
    if(p.waitFor()!=0) {
      throw new Exception("Setup incomplete. " + command + " FAILED");
    }
    else {
      LOGGER.info("Command:" + command + " PASSED");
    }

    command = "hdfs dfs -rm " + hdfsPath;
    p = Runtime.getRuntime().exec(command);
    status = (p.waitFor()==0)?"PASSED":"FAILED";
    LOGGER.warn("Command:" + command + " " + status);

    command = "hdfs dfs -put " + file.getAbsolutePath() + " " + hdfsPath;
    p = Runtime.getRuntime().exec(command);
    if(p.waitFor()!=0) {
      throw new Exception("Setup incomplete. " + command + " FAILED");
    }
    else {
      LOGGER.info("Command:" + command + " PASSED");
    }
  }

  private String getSection(String name, Map<String, String> mapping) {
    if(mapping.isEmpty()) {
      return "";
    }
    Joiner kvJoiner = Joiner.on(" = ");
    List<String> lines = Lists.newArrayList();
    lines.add("[" + name + "]");
    for(String key : mapping.keySet()) {
      lines.add(kvJoiner.join(key, mapping.get(key)));
    }
    return Joiner.on(NL).join(lines);
  }
  private String getSection(String name, Multimap<String, String> mapping) {
    if(mapping.isEmpty()) {
      return "";
    }
    Joiner kvJoiner = Joiner.on(" = ");
    Joiner itemJoiner = Joiner.on(" , ");
    List<String> lines = Lists.newArrayList();
    lines.add("[" + name + "]");
    for(String key : mapping.keySet()) {
      lines.add(kvJoiner.join(key, itemJoiner.join(mapping.get(key))));
    }
    return Joiner.on(NL).join(lines);
  }

  private PolicyFile remove(Collection<String> exitingItems, String[] newItems) {
    for(String newItem : newItems) {
      if(!exitingItems.remove(newItem)) {
        throw new IllegalStateException("Item " + newItem + " did not exist in " + exitingItems);
      }
    }
    return this;
  }
  private PolicyFile add(Collection<String> exitingItems, boolean allowDuplicates, String[] newItems) {
    for(String newItem : newItems) {
      if(exitingItems.contains(newItem) && !allowDuplicates) {
        throw new IllegalStateException("Item " + newItem + " already exists in " + exitingItems);
      }
      exitingItems.add(newItem);
    }
    return this;
  }

  //User:Group mapping for the admin user needs to be set separately
  public static PolicyFile setAdminOnServer1(String admin) {
    return new PolicyFile()
      .addRolesToGroup(admin, "admin_role")
      .addPermissionsToRole("admin_role", "server=server1");
  }
}
