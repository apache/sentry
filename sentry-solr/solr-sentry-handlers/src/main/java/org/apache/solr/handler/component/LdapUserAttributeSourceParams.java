/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.handler.component;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import org.apache.solr.common.params.SolrParams;

public class LdapUserAttributeSourceParams implements UserAttributeSourceParams {

  private String baseDn;
  private String userFilter;
  private String serverUrl;
  private String authType;
  private String username;
  private String password;
  private boolean enableStartTls;
  private boolean disableHostNameVerification;

  private boolean doNestedQuery;     // Whether to recursively follow recursiveAttribute to find parent groups
  private String recursiveAttribute; // Only required if doNestedQuery==true
  private int maxRecurseDepth;       // Only used if doNestedQuery==true
  private long groupCacheTtl;
  private long groupCacheMaxSize;

  public static final String LDAP_ADMIN_USER = "ldapAdminUser";
  public static final String LDAP_ADMIN_PASSWORD = "ldapAdminPassword";
  public static final String LDAP_AUTH_TYPE = "ldapAuthType";
  public static final String LDAP_AUTH_TYPE_DEFAULT = "none";
  public static final String LDAP_BASE_DN = "ldapBaseDN";
  public static final String LDAP_TLS_ENABLED = "ldapTlsEnabled";
  public static final boolean LDAP_TLS_ENABLED_DEFAULT = false;
  public static final String LDAP_TLS_DISABLE_HOSTNAME_VERIFICATION = "ldapTlsDisableHostnameVerification";
  public static final boolean LDAP_TLS_DISABLE_HOSTNAME_VERIFICATION_DEFAULT = false;
  public static final String LDAP_USER_SEARCH_FILTER = "ldapUserSearchFilter";
  public static final String LDAP_USER_SEARCH_FILTER_DEFAULT = "(uid={0})";
  public static final String LDAP_PROVIDER_URL = "ldapProviderUrl";

  // Properties for handling nested access groups recursively
  public static final String LDAP_NESTED_GROUPS_ENABLED = "ldapNestedGroupsEnabled";
  public static final boolean LDAP_NESTED_GROUPS_ENABLED_DEFAULT = false; // Disabled by default for performance reasons
  public static final String LDAP_RECURSIVE_ATTRIBUTE = "ldapRecursiveAttribute";
  public static final String LDAP_RECURSIVE_ATTRIBUTE_DEFAULT = "memberOf";
  public static final String LDAP_MAX_RECURSE_DEPTH = "ldapMaxRecurseDepth";
  public static final int LDAP_MAX_RECURSE_DEPTH_DEFAULT = 5;

  // Caching of access groups to reduce number of LDAP queries for nested groups
  public static final String LDAP_GROUP_CACHE_TTL_SECONDS = "ldapGroupCacheTtlSeconds";
  public static final long LDAP_GROUP_CACHE_TTL_SECONDS_DEFAULT = 30;
  public static final String LDAP_GROUP_CACHE_MAX_SIZE = "ldapGroupCacheMaxSize";
  public static final long LDAP_GROUP_CACHE_MAX_SIZE_DEFAULT = 1000;

  public String getAuthType() {
    return authType;
  }

  public void setAuthType(String authType) {
    Preconditions.checkNotNull(authType);
    this.authType = authType;
  }

  public String getBaseDn() {
    return baseDn;
  }

  public void setBaseDn(String baseDn) {
    Preconditions.checkNotNull(baseDn);
    this.baseDn = baseDn;
  }

  public String getPassword() {
    return password;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  public String getServerUrl() {
    return serverUrl;
  }

  public void setServerUrl(String serverUrl) {
    Preconditions.checkNotNull(serverUrl);
    this.serverUrl = serverUrl;
  }

  public String getUserFilter() {
    return userFilter;
  }

  public void setUserFilter(String userFilter) {
    Preconditions.checkNotNull(userFilter);
    this.userFilter = userFilter;
  }

  public String getUsername() {
    return username;
  }

  public void setUsername(String username) {
    this.username = username;
  }

  public boolean isHostNameVerificationDisabled() {
    return disableHostNameVerification;
  }

  public void setDisableHostNameVerification(boolean disableHostNameVerification) {
    this.disableHostNameVerification = disableHostNameVerification;
  }

  public boolean isStartTlsEnabled() {
    return enableStartTls;
  }

  public void setEnableStartTls(boolean enableStartTls) {
    this.enableStartTls = enableStartTls;
  }

  public boolean isNestedQueryEnabled() {
    return doNestedQuery;
  }

  public void setNestedQueryEnabled(boolean doNestedQuery) {
    this.doNestedQuery = doNestedQuery;
  }

  public String getRecursiveAttribute() {
    return recursiveAttribute;
  }

  public void setRecursiveAttribute(String attr) {
    this.recursiveAttribute = attr;
  }

  public int getMaxRecurseDepth() {
    return maxRecurseDepth;
  }

  public void setMaxRecurseDepth(int maxDepth) {
    this.maxRecurseDepth = maxDepth;
  }

  public long getGroupCacheTtl() {
    return groupCacheTtl;
  }

  public void setGroupCacheTtl(long groupCacheTtl) {
    this.groupCacheTtl = groupCacheTtl;
  }

  public long getGroupCacheMaxSize() {
    return groupCacheMaxSize;
  }

  public void setGroupCacheMaxSize(long groupCacheMaxSize) {
    this.groupCacheMaxSize = groupCacheMaxSize;
  }

  // Note that equals(), hashCode() and toString() currently only use a subset of the attributes above - do they need extending?

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    LdapUserAttributeSourceParams that = (LdapUserAttributeSourceParams) o;
    return Objects.equal(baseDn, that.baseDn) &&
        Objects.equal(userFilter, that.userFilter) &&
        Objects.equal(serverUrl, that.serverUrl) &&
        Objects.equal(authType, that.authType) &&
        Objects.equal(username, that.username) &&
        Objects.equal(password, that.password);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(baseDn, userFilter, serverUrl, authType, username, password);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("authType", authType)
        .add("baseDn", baseDn)
        .add("userFilter", userFilter)
        .add("serverUrl", serverUrl)
        .add("username", username)
        .add("password", "***")
        .toString();
  }

  @Override
  public void init(SolrParams solrParams) {
    setServerUrl(solrParams.get(LDAP_PROVIDER_URL));
    setBaseDn(solrParams.get(LDAP_BASE_DN));
    setUserFilter(solrParams.get(LDAP_USER_SEARCH_FILTER, LDAP_USER_SEARCH_FILTER_DEFAULT));
    setAuthType(solrParams.get(LDAP_AUTH_TYPE, LDAP_AUTH_TYPE_DEFAULT));
    setUsername(solrParams.get(LDAP_ADMIN_USER));
    setPassword(solrParams.get(LDAP_ADMIN_PASSWORD));
    setEnableStartTls(solrParams.getBool(LDAP_TLS_ENABLED, LDAP_TLS_ENABLED_DEFAULT));
    setDisableHostNameVerification(solrParams.getBool(LDAP_TLS_DISABLE_HOSTNAME_VERIFICATION, LDAP_TLS_DISABLE_HOSTNAME_VERIFICATION_DEFAULT));
    setNestedQueryEnabled(solrParams.getBool(LDAP_NESTED_GROUPS_ENABLED, LDAP_NESTED_GROUPS_ENABLED_DEFAULT));
    setRecursiveAttribute(solrParams.get(LDAP_RECURSIVE_ATTRIBUTE, LDAP_RECURSIVE_ATTRIBUTE_DEFAULT));
    setMaxRecurseDepth(solrParams.getInt(LDAP_MAX_RECURSE_DEPTH, LDAP_MAX_RECURSE_DEPTH_DEFAULT));
    setGroupCacheMaxSize(solrParams.getLong(LDAP_GROUP_CACHE_MAX_SIZE, LDAP_GROUP_CACHE_MAX_SIZE_DEFAULT));
    setGroupCacheTtl(solrParams.getLong(LDAP_GROUP_CACHE_TTL_SECONDS, LDAP_GROUP_CACHE_TTL_SECONDS_DEFAULT));
  }
}
