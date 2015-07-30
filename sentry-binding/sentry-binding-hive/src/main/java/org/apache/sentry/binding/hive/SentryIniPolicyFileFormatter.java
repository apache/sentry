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

package org.apache.sentry.binding.hive;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.provider.common.PolicyFileConstants;
import org.apache.sentry.provider.common.ProviderBackendContext;
import org.apache.sentry.provider.common.ProviderConstants;
import org.apache.sentry.provider.file.SimpleFileProviderBackend;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;
import com.google.common.io.Files;

/**
 * SentryIniPolicyFileFormatter is to parse file and write data to file for sentry mapping data with
 * ini format, eg:
 * [groups]
 * group1=role1
 * [roles]
 * role1=server=server1
 */
public class SentryIniPolicyFileFormatter implements SentryPolicyFileFormatter {

  private static final Logger LOGGER = LoggerFactory.getLogger(SentryIniPolicyFileFormatter.class);

  private static final String NL = System.getProperty("line.separator", "\n");

  /**
   * Write the sentry mapping data to ini file.
   * 
   * @param resourcePath
   *        The path of the output file
   * @param sentryMappingData
   *        The map for sentry mapping data, eg:
   *        for the following mapping data:
   *        group1=role1,role2
   *        group2=role2,role3
   *        role1=server=server1->db=db1
   *        role2=server=server1->db=db1->table=tbl1,server=server1->db=db1->table=tbl2
   *        role3=server=server1->url=hdfs://localhost/path
   * 
   *        The sentryMappingData will be inputed as:
   *        {
   *        groups={[group1={role1, role2}], group2=[role2, role3]},
   *        roles={role1=[server=server1->db=db1],
   *        role2=[server=server1->db=db1->table=tbl1,server=server1->db=db1->table=tbl2],
   *        role3=[server=server1->url=hdfs://localhost/path]
   *        }
   *        }
   */
  @Override
  public void write(String resourcePath, Map<String, Map<String, Set<String>>> sentryMappingData)
      throws Exception {
    File destFile = new File(resourcePath);
    if (destFile.exists() && !destFile.delete()) {
      throw new IllegalStateException("Unable to delete " + destFile);
    }
    String contents = Joiner
        .on(NL)
        .join(
        generateSection(PolicyFileConstants.GROUPS,
                sentryMappingData.get(PolicyFileConstants.GROUPS)),
        generateSection(PolicyFileConstants.ROLES,
                sentryMappingData.get(PolicyFileConstants.ROLES)),
            "");
    LOGGER.info("Writing policy file to " + destFile + ":\n" + contents);
    Files.write(contents, destFile, Charsets.UTF_8);
  }

  /**
   * parse the ini file and return a map with all data
   * 
   * @param resourcePath
   *        The path of the input file
   * @param conf
   *        The configuration info
   * @return the result of sentry mapping data in map structure.
   */
  @Override
  public Map<String, Map<String, Set<String>>> parse(String resourcePath, Configuration conf)
      throws Exception {
    Map<String, Map<String, Set<String>>> resultMap = Maps.newHashMap();
    // SimpleFileProviderBackend is used for parse the ini file
    SimpleFileProviderBackend policyFileBackend = new SimpleFileProviderBackend(conf, resourcePath);
    ProviderBackendContext context = new ProviderBackendContext();
    context.setAllowPerDatabase(true);
    // parse the ini file
    policyFileBackend.initialize(context);

    // SimpleFileProviderBackend parsed the input file and output the data in Table format.
    Table<String, String, Set<String>> groupRolePrivilegeTable = policyFileBackend
        .getGroupRolePrivilegeTable();
    Map<String, Set<String>> groupRolesMap = Maps.newHashMap();
    Map<String, Set<String>> rolePrivilegesMap = Maps.newHashMap();
    for (String groupName : groupRolePrivilegeTable.rowKeySet()) {
      for (String roleName : groupRolePrivilegeTable.columnKeySet()) {
        // get the roles set for the current groupName
        Set<String> tempRoles = groupRolesMap.get(groupName);
        if (tempRoles == null) {
          tempRoles = Sets.newHashSet();
        }
        Set<String> privileges = groupRolePrivilegeTable.get(groupName, roleName);
        // if there has privilege for [group,role], if no privilege exist, the [group, role] info
        // will be discard.
        if (privileges != null) {
          // update [group, role] mapping data
          tempRoles.add(roleName);
          groupRolesMap.put(groupName, tempRoles);
          // update [role, privilege] mapping data
          rolePrivilegesMap.put(roleName, privileges);
        }
      }
    }
    resultMap.put(PolicyFileConstants.GROUPS, groupRolesMap);
    resultMap.put(PolicyFileConstants.ROLES, rolePrivilegesMap);
    return resultMap;
  }

  // generate the ini section according to the mapping data.
  private String generateSection(String name, Map<String, Set<String>> mappingData) {
    if (mappingData.isEmpty()) {
      return "";
    }
    List<String> lines = Lists.newArrayList();
    lines.add("[" + name + "]");
    for (String key : mappingData.keySet()) {
      lines.add(ProviderConstants.KV_JOINER.join(key,
          ProviderConstants.ROLE_JOINER.join(mappingData.get(key))));
    }
    return Joiner.on(NL).join(lines);
  }

}
