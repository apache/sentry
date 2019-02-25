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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Multimap;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;
import javax.naming.ldap.InitialLdapContext;
import javax.naming.ldap.LdapContext;
import javax.naming.ldap.StartTlsRequest;
import javax.naming.ldap.StartTlsResponse;
import javax.net.ssl.HostnameVerifier;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static javax.naming.Context.INITIAL_CONTEXT_FACTORY;
import static javax.naming.Context.PROVIDER_URL;
import static javax.naming.Context.SECURITY_AUTHENTICATION;
import static javax.naming.Context.SECURITY_CREDENTIALS;
import static javax.naming.Context.SECURITY_PRINCIPAL;

/**
 * This class implements the logic to extract attributes for a specified user
 * from LDAP server. Currently this class supports following modes of authentication
 * with the LDAP server,
 * - none (anonymous bind operation)
 * - simple (based on username and password).
 * - GSSAPI (kerberos)
 *
 * This filter accepts following parameters for interacting with LDAP server,
 * ldapProviderUrl : Url of the LDAP server (e.g. ldap://myserver)
 * ldapAuthType : Type of the authentication mechanism used to connect to LDAP server.
 *                Currently supported values: simple or none
 * ldapAdminUser : DN of the LDAP admin user (only applicable in case of "simple" authentication)
 * ldapAdminPassword : password of the LDAP admin user (only applicable in case of "simple" authentication)
 * ldapBaseDN : Base DN string to be used to query LDAP server. The implementation
 *              prepends user name (i.e. uid=<user_name>) to this value for querying the
 *              attributes.
 *
 * This class supports extracting single (or multi-valued) String attributes only. The
 * raw value(s) of the attribute are used for document-level filtering.
 */
public class LdapUserAttributeSource implements UserAttributeSource {

  private static final HostnameVerifier PERMISSIVE_HOSTNAME_VERIFIER = (hostname, session) -> true;
  private static final Logger LOG = LoggerFactory.getLogger(LdapUserAttributeSource.class);

  /**
   * Singleton cache provides the parent group(s) for a given group.
   * The cache classes are threadsafe so can be shared by multiple LdapUserAttributeSource instances.
   */
  private static volatile Cache<String, Set<String>> scache;
  private static final Object SCACHE_SYNC = new Object();

  public static Cache<String, Set<String>> getCache(long ttlSeconds, long maxCacheSize) {
    if (scache != null) {
      return scache;
    }
    synchronized (SCACHE_SYNC) {
      if (scache == null) {
        LOG.info("Creating access group cache, ttl={} maxSize={}", ttlSeconds, maxCacheSize);
        scache = CacheBuilder.newBuilder().expireAfterWrite(ttlSeconds, TimeUnit.SECONDS).maximumSize(maxCacheSize).build(); // No auto-load in case of a cache miss - must populate explicitly
      }
      return scache;
    }
  }

  @SuppressWarnings({"rawtypes", "PMD.ReplaceHashtableWithMap"})
  private Hashtable env;
  private LdapUserAttributeSourceParams params;
  private SearchControls searchControls;

  // Per-instance copy of the static singleton cache
  private Cache<String, Set<String>> cache;

  public void init(UserAttributeSourceParams params, Collection<String> attributes) {
    LOG.debug("Creating LDAP user attribute source, params={}, attributes={}", params, attributes);

    if (!(params instanceof LdapUserAttributeSourceParams)) {
      throw new SolrException(ErrorCode.INVALID_STATE, "LdapUserAttributeSource has been misconfigured with the wrong parameters {" + params.getClass().getName() + "}");
    }

    this.params = (LdapUserAttributeSourceParams) params;
    this.env = toEnv(this.params);

    searchControls = new SearchControls();
    searchControls.setReturningAttributes(attributes.toArray(new String[attributes.size()]));
    searchControls.setSearchScope(SearchControls.SUBTREE_SCOPE);

    if (this.params.isStartTlsEnabled() && this.params.getServerUrl().startsWith("ldaps://")) {
      throw new SolrException(ErrorCode.SERVER_ERROR, "Start TLS should not be used with ldaps://");
    }

    long cacheTtl = this.params.getGroupCacheTtl();
    long cacheMaxSize = this.params.getGroupCacheMaxSize();
    cache = getCache(cacheTtl, cacheMaxSize); // Singleton; only the first spec seen will be used
  }

  @SuppressWarnings({"rawtypes", "unchecked", "PMD.ReplaceHashtableWithMap"})
  private Hashtable toEnv(LdapUserAttributeSourceParams params) {
    final Hashtable result = new Hashtable();
    result.put(INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
    result.put(PROVIDER_URL, params.getServerUrl());
    String authType = params.getAuthType();
    result.put(SECURITY_AUTHENTICATION, authType);
    if ("simple".equals(authType)) {
      result.put(SECURITY_PRINCIPAL, params.getUsername());
      result.put(SECURITY_CREDENTIALS, params.getPassword());
    }
    return result;
  }

  /**
   * Return specified LDAP attributes for the user identified by <code>userName</code>
   */
  @Override
  @SuppressWarnings({"rawtypes", "unchecked", "PMD.ReplaceHashtableWithMap"})
  public Multimap<String, String> getAttributesForUser(String userName) {
    LdapContext ctx = null;
    try {
      ctx = new InitialLdapContext(env, null);
      Multimap<String, String> result;
      if (params.isStartTlsEnabled()) {
        StartTlsResponse tls = (StartTlsResponse) ctx.extendedOperation(new StartTlsRequest());
        if (params.isHostNameVerificationDisabled()) {
          tls.setHostnameVerifier(PERMISSIVE_HOSTNAME_VERIFIER);
        }
        tls.negotiate();
        result = doAttributeSearch(userName, ctx);
        tls.close();
      } else {
        result = doAttributeSearch(userName, ctx);
      }
      return result;
    } catch (NamingException | IOException e) {
      throw new SolrException(ErrorCode.SERVER_ERROR, "Unable to query LDAP server", e);
    } finally {
      if (ctx != null) {
        try {
          ctx.close();
        } catch (NamingException ignored) {
        }
      }
    }
  }

  @Override
  public Class<LdapUserAttributeSourceParams> getParamsClass() {
    return LdapUserAttributeSourceParams.class;
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private Multimap<String, String> doAttributeSearch(String userName, LdapContext ctx) throws NamingException {
    NamingEnumeration searchResults = ctx.search(params.getBaseDn(), params.getUserFilter().replace("{0}", userName), searchControls);
    if (!searchResults.hasMore()) {
      LOG.error("User '{}' not found in LDAP", userName);
      throw new SolrException(ErrorCode.SERVER_ERROR, "User not found in LDAP");
    }
    LOG.info("Fetching attributes for {} from LDAP using {}", userName, this);
    Multimap<String, String> result = LinkedHashMultimap.create(); // NB LinkedHashMultimap does not allow duplicate values.
    while (searchResults.hasMore()) {
      SearchResult entry = (SearchResult) searchResults.next();
      Attributes attributes = entry.getAttributes();
      NamingEnumeration<? extends Attribute> attrEnum = attributes.getAll();
      while (attrEnum.hasMore()) {
        Attribute a = attrEnum.next();
        NamingEnumeration values = a.getAll();
        while (values.hasMore()) {
          result.put(a.getID(), (String) values.next());
        }
      }
    }
    LOG.debug("Direct attributes found for user {}: {}", userName, result);

    // Optionally, recurse along the specified property such as "memberOf" to find indirect (nested) group memberships.
    // A maxDepth of 1 indicates that we find direct groups and parent groups. A maxDepth of 2 also includes grandparents, etc.
    if (params.isNestedQueryEnabled() && params.getMaxRecurseDepth() > 0) {
      LOG.debug("Querying nested groups for user {} up to depth {}", userName, params.getMaxRecurseDepth());
      String recursiveAttr = params.getRecursiveAttribute(); // Configurable, but typically "memberOf"
      // Important: take a defensive copy of the original groups, because we modify the Multimap inside the loop:
      Set<String> values = new HashSet(result.get(recursiveAttr)); //  Multimap.get() is never null
      for (String group : values) {
          Set<String> known = new HashSet<>();
          known.add(group); // avoid cycles to self.
          getParentGroups(group, known, ctx, 1); // modifies the 'known' Set, adding any new groups found
          known.remove(group); // We already have this group in the result, or we wouldn't be here. Remove for clarity of logging:
          LOG.debug("Adding parent groups for {} : {}", group, known);
          result.putAll(recursiveAttr, known);
      }
      LOG.debug("Total attributes found for user {}: {}", userName, result);
    }
    return result;
  }

  /**
   * Recursively find the parent groups (if any) of the specified group, and add them to the user's direct attribute,
   * so that we can return both direct and indirect group memberships. Limit the depth of recursion. Detect and avoid loops.
   *
   * As a special case, caches the top-level "ancestor" groups (which have no parent groups) to reduce the number of LDAP queries.
   * Does not cache other groups because, in general, the cycle-detection may terminate recursion and prevent us from getting
   * and thus caching a full picture of the groups reachable from a given group. It would also be possible to cache the bottom-level
   * groups in the main loop in doAttributeSearch, but this is not implemented at present.
   */
  @SuppressWarnings({"rawtypes", "unchecked"})
  private Set<String> getParentGroups(String childGroup, Set<String> knownGroups, LdapContext ctx, int depth) throws NamingException {
    // Throw an exception if we have exceeded the recursion depth; this causes the entire query to fail.
    // This alerts us that the recursion depth is insufficient to traverse all of the user's nested groups
    // and will need increasing (or the group structure will need to be simplified).
    if (depth > params.getMaxRecurseDepth()) {
      // E.g. maxRecurseDepth=1 means that only direct groups are allowed; no parent groups can be handled
      // maxRecurseDepth=3 means that direct groups, parent, and grandparent groups can be handled
      throw new SolrException(ErrorCode.SERVER_ERROR, "Nested groups recursion limit exceeded for group " + childGroup + " at depth " + depth);
    }

    // First try the cache:
    Set<String> parents = cache.getIfPresent(childGroup); // nullable
    if (parents != null) {
      LOG.debug("Cache hit for {} : {}", childGroup, parents); // Currently we only cache "ancestor" groups, which have NO parents!
      knownGroups.addAll(parents); // Added since 0.0.6-SNAPSHOT delivered to site; important fix for when we start caching non-ancestor groups
      return parents; // cache hit
    } else { // Cache miss
     LOG.debug("Querying LDAP for parent groups of {} at depth {}...", childGroup, depth);
      String recursiveAttr = params.getRecursiveAttribute(); // Configurable, but typically "memberOf"

      // There may be potential optimisations here if we can skip queries for some groups
      // depending on parts of their CN, e.g. for CN=Builtin groups *if* they are known to be top-level groups.

      Attributes atts = ctx.getAttributes(childGroup, new String[] {recursiveAttr}); // attributes such as memberOf will return the DN; the regex in solrconfig.xml will need to extract the CN from the DN.
      Attribute att = atts.get(recursiveAttr);
      if (att != null && att.size() > 0) {
        LOG.debug("Group {} has direct parent groups: {}", childGroup, att);
        NamingEnumeration<?> parentGroups = att.getAll();
        while (parentGroups.hasMore()) {
          String parentGroup = parentGroups.next().toString();
          // Skip recursion if we've seen this group before; avoid cycles and multiple paths through same ancestors
          if (knownGroups.add(parentGroup)) {
            LOG.debug("Found new parent group: {} - recursing...", parentGroup);
            // Recurse until we find a group that has no parents, or we hit the depth limit (which throws an Exception above)
            getParentGroups(parentGroup, knownGroups, ctx, depth + 1);
            // Don't cache these results! They may be incomplete because cycle detection will stop recursion early.
          } else {
            LOG.debug("Cycle detected for parent group: {} - stopping recursion.", parentGroup);
          }
        }
      } else {
        LOG.debug("No parent groups found for group {}", childGroup);
        cache.put(childGroup, Collections.emptySet()); // This is an "ancestor" group with no parents
      }
      return knownGroups;
    }
  }
}
