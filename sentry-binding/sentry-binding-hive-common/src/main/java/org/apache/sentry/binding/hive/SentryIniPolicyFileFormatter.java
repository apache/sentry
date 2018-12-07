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

import java.io.BufferedWriter;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.sentry.core.common.exception.SentryConfigurationException;
import org.apache.sentry.core.common.utils.SentryConstants;
import org.apache.sentry.core.common.utils.PolicyFileConstants;
import org.apache.sentry.core.common.utils.PolicyFiles;
import org.apache.sentry.service.common.ServiceConstants;
import org.apache.shiro.config.Ini;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

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
   * @param resourcePath The path of the output file
   * @param conf sentry configuration
   * @param sentryMappingData
   *        The map for sentry mapping data, eg:
   *        for the following mapping data:
   *        user1=role1,role2
   *        user2=role2,role3
   *        group1=role1,role2
   *        group2=role2,role3
   *        role1=server=server1->db=db1
   *        role2=server=server1->db=db1->table=tbl1,server=server1->db=db1->table=tbl2
   *        role3=server=server1->url=hdfs://localhost/path
   *
   *        The sentryMappingData will be inputed as:
   *        {
   *        users={[user1={role1, role2}], group2=[role2, role3]},
   *        groups={[group1={role1, role2}], group2=[role2, role3]},
   *        roles={role1=[server=server1->db=db1],
   *        role2=[server=server1->db=db1->table=tbl1,server=server1->db=db1->table=tbl2],
   *        role3=[server=server1->url=hdfs://localhost/path]
   *        }
   *        }
   */
  @Override
  public void write(String resourcePath, Configuration conf, Map<String, Map<String, Set<String>>> sentryMappingData)
      throws Exception {
    Path path = new Path(resourcePath);
    if (Strings.isNullOrEmpty(path.toUri().getScheme())) {
      // Path provided did not have any URI scheme. Update the scheme based on configuration.
      String defaultFs = conf.get(ServiceConstants.ClientConfig.SENTRY_EXPORT_IMPORT_DEFAULT_FS,
              ServiceConstants.ClientConfig.SENTRY_EXPORT_IMPORT_DEFAULT_FS_DEFAULT);
       path = new Path(defaultFs + resourcePath);
    }

    FileSystem fileSystem = path.getFileSystem(conf);
    if (fileSystem.exists(path)) {
      fileSystem.delete(path,true);
    }
    String contents = Joiner
        .on(NL)
        .join(
        generateSection(PolicyFileConstants.USER_ROLES,
            sentryMappingData.get(PolicyFileConstants.USER_ROLES)),
        generateSection(PolicyFileConstants.GROUPS,
                sentryMappingData.get(PolicyFileConstants.GROUPS)),
        generateSection(PolicyFileConstants.ROLES,
                sentryMappingData.get(PolicyFileConstants.ROLES)),
            "");
    LOGGER.info("Writing policy information to file located at" + path);
    OutputStream os = fileSystem.create(path);
    BufferedWriter br = new BufferedWriter( new OutputStreamWriter( os, "UTF-8" ) );
    try {
      br.write(contents);
    }
    catch (Exception exception) {
      LOGGER.error("Failed to export policy information to file, located at " + path);
    }
    finally {
      br.close();
      fileSystem.close();
    }
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
  public Map<String, Map<String, Set<String>>> parse(String resourcePath, Configuration conf) {
    Map<String, Map<String, Set<String>>> resultMap = Maps.newHashMap();
    Path path = new Path(resourcePath);
    Ini ini;

    try {
      ini = PolicyFiles.loadFromPath(path.getFileSystem(conf), path);
    } catch (Exception e) {
      throw new SentryConfigurationException("Error loading policy file "
          + resourcePath, e);
    }
    Map<String, Set<String>> userRolesMap = parseSection(ini,
        PolicyFileConstants.USER_ROLES);
    Map<String, Set<String>> groupRolesMap = parseSection(ini,
        PolicyFileConstants.GROUPS);
    Map<String, Set<String>> rolePrivilegesMap = parseSection(ini,
        PolicyFileConstants.ROLES);
    resultMap.put(PolicyFileConstants.USER_ROLES, userRolesMap);
    resultMap.put(PolicyFileConstants.GROUPS, groupRolesMap);
    resultMap.put(PolicyFileConstants.ROLES, rolePrivilegesMap);
    return resultMap;
  }

  private Map<String, Set<String>> parseSection(Ini ini, String sctionName) {
    Map<String, Set<String>> resultMap = Maps.newHashMap();
    Ini.Section sction = ini.getSection(sctionName);
    if (sction == null) {
      return resultMap;
    }
    for (String key : sction.keySet()) {
      String value = sction.get(key);
      Set<String> roles = Sets.newHashSet();
      for (String role : value.split(SentryConstants.ROLE_SEPARATOR)) {
        if (StringUtils.isNotEmpty(role)) {
          roles.add(role);
        }
      }
      resultMap.put(key, roles);
    }
    return resultMap;
  }

  // generate the ini section according to the mapping data.
  private String generateSection(String name, Map<String, Set<String>> mappingData) {
    if (mappingData == null || mappingData.isEmpty()) {
      return "";
    }
    List<String> lines = Lists.newArrayList();
    lines.add("[" + name + "]");
    for (Map.Entry<String, Set<String>> entry : mappingData.entrySet()) {
      lines.add(SentryConstants.KV_JOINER.join(entry.getKey(),
          SentryConstants.ROLE_JOINER.join(entry.getValue())));
    }
    return Joiner.on(NL).join(lines);
  }

}
