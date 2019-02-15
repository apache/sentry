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

import com.google.common.base.Joiner;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Set;

public class QueryDocAuthorizationComponent extends DocAuthorizationComponent {
  private static final Logger LOG =
    LoggerFactory.getLogger(QueryDocAuthorizationComponent.class);
  public static final String AUTH_FIELD_PROP = "sentryAuthField";
  public static final String DEFAULT_AUTH_FIELD = "sentry_auth";
  public static final String ALL_ROLES_TOKEN_PROP = "allRolesToken";
  public static final String ENABLED_PROP = "enabled";
  public static final String MODE_PROP = "matchMode";
  public static final String DEFAULT_MODE_PROP = MatchType.DISJUNCTIVE.toString();

  public static final String ALLOW_MISSING_VAL_PROP = "allow_missing_val";
  public static final String TOKEN_COUNT_PROP = "tokenCountField";
  public static final String DEFAULT_TOKEN_COUNT_FIELD_PROP = "sentry_auth_count";
  public static final String QPARSER_PROP = "qParser";


  private String authField;
  private String allRolesToken;
  private boolean enabled;
  private MatchType matchMode;
  private String tokenCountField;
  private boolean allowMissingValue;

  private String qParserName;

  private enum MatchType {
    DISJUNCTIVE,
    CONJUNCTIVE
  }

  @Override
  public void init(NamedList args) {
    SolrParams params = args.toSolrParams();
    this.authField = params.get(AUTH_FIELD_PROP, DEFAULT_AUTH_FIELD);
    LOG.info("QueryDocAuthorizationComponent authField: {}", this.authField);
    this.allRolesToken = params.get(ALL_ROLES_TOKEN_PROP, "");
    LOG.info("QueryDocAuthorizationComponent allRolesToken: {}", this.allRolesToken);
    this.enabled = params.getBool(ENABLED_PROP, false);
    LOG.info("QueryDocAuthorizationComponent enabled: {}", this.enabled);
    this.matchMode = MatchType.valueOf(params.get(MODE_PROP, DEFAULT_MODE_PROP).toUpperCase());
    LOG.info("QueryDocAuthorizationComponent matchType: {}", this.matchMode);

    if (this.matchMode == MatchType.CONJUNCTIVE) {
      this.qParserName = params.get(QPARSER_PROP, "subset").trim();
      LOG.debug("QueryDocAuthorizationComponent qParserName: {}", this.qParserName);
      this.allowMissingValue = params.getBool(ALLOW_MISSING_VAL_PROP, false);
      LOG.debug("QueryDocAuthorizationComponent allowMissingValue: {}", this.allowMissingValue);
      this.tokenCountField = params.get(TOKEN_COUNT_PROP, DEFAULT_TOKEN_COUNT_FIELD_PROP);
      LOG.debug("QueryDocAuthorizationComponent tokenCountField: {}", this.tokenCountField);
    }
  }

  private void addDisjunctiveRawClause(StringBuilder builder, String value) {
    // requires a space before the first term, so the
    // default lucene query parser will be used
    builder.append(" {!raw f=").append(authField).append(" v=")
      .append(value).append("}");
  }

  public String getDisjunctiveFilterQueryStr(Set<String> roles) {
    if (roles != null && !roles.isEmpty()) {
      StringBuilder builder = new StringBuilder();
      for (String role : roles) {
        addDisjunctiveRawClause(builder, role);
      }
      if (allRolesToken != null && !allRolesToken.isEmpty()) {
        addDisjunctiveRawClause(builder, allRolesToken);
      }
      return builder.toString();
    }
    return null;
  }

  @Override
  public void prepare(ResponseBuilder rb, String userName) throws IOException {
    Set<String> roles = getRoles(rb.req, userName);
    if (roles != null && !roles.isEmpty()) {
      String filterQuery;
      if (matchMode == MatchType.DISJUNCTIVE) {
        filterQuery = getDisjunctiveFilterQueryStr(roles);
      } else {
        filterQuery = getConjunctiveFilterQueryStr(roles);
      }
      ModifiableSolrParams newParams = new ModifiableSolrParams(rb.req.getParams());
      newParams.add("fq", filterQuery);
      rb.req.setParams(newParams);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Adding filter query {} for user {} with roles {}", filterQuery, userName, roles);
      }

    } else {
      throw new SolrException(SolrException.ErrorCode.UNAUTHORIZED,
        "Request from user: " + userName + " rejected because user is not associated with any roles");
    }
  }

  private String getConjunctiveFilterQueryStr(Set<String> roles) {
    StringBuilder filterQuery = new StringBuilder();
    filterQuery
        .append(" {!").append(qParserName)
        .append(" set_field=\"").append(authField).append("\"")
        .append(" set_value=\"").append(Joiner.on(',').join(roles.iterator())).append("\"")
        .append(" count_field=\"").append(tokenCountField).append("\"");
    if (allRolesToken != null && !allRolesToken.equals("")) {
      filterQuery.append(" wildcard_token=\"").append(allRolesToken).append("\"");
    }
    filterQuery.append(" allow_missing_val=").append(allowMissingValue).append(" }");

    return filterQuery.toString();
  }

  @Override
  public void process(ResponseBuilder rb) throws IOException {
  }

  @Override
  public String getDescription() {
    return "Handle Query Document Authorization";
  }

  @Override
  public boolean getEnabled() {
    return enabled;
  }
}
