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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Multimap;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This custom {@linkplain SearchComponent} is responsible to introduce
 * a document level security filter based on values of user attributes associated
 * with the authenticated user.
 *
 * This filter also configures a {@Linkplain UserAttributeSource} via parameters evaluated by
 * a {@Linkplan UserAttributeSourceParams}. All UserAttributeSourceParams should be initialized here
 * andQParser : Name of the registered QParser used for subset matching (required for AND type filter).
 * field_attr_mappings - A list mapping Solr index field -> user attribute on which the document level
 *                      security is to be implemented. For each mapping, we need to specify -
 *                      - name of the field in the Solr index
 *                      - A comma-separated string of user attribute name (and its aliases).
 *                      - Type of filter to be applied. Currently we support OR and AND based filters.
 */
public class SolrAttrBasedFilter extends DocAuthorizationComponent {

  private static final Logger LOG = LoggerFactory.getLogger(SolrAttrBasedFilter.class);

  public static final String CACHE_ENABLED_PROP = "cache_enabled";
  public static final boolean CACHE_ENABLED_DEFAULT = false;
  public static final String CACHE_TTL_PROP = "cache_ttl_seconds";
  public static final long CACHE_TTL_DEFAULT = 30;
  public static final String CACHE_MAX_SIZE_PROP = "cache_max_size";
  public static final long CACHE_MAX_SIZE_DEFAULT = 1000;
  public static final String ENABLED_PROP = "enabled";
  public static final String FIELD_ATTR_MAPPINGS = "field_attr_mappings";

  public static final String USER_ATTRIBUTE_SOURCE_CLASSNAME = "userAttributeSource";
  public static final String USER_ATTRIBUTE_SOURCE_CLASSNAME_DEFAULT = "org.apache.solr.handler.component.LdapUserAttributeSource";

  public static final String FIELD_FILTER_TYPE = "filter_type";
  public static final String ATTR_NAMES = "attr_names";
  public static final String PERMIT_EMPTY_VALUES = "permit_empty";
  public static final String ALL_USERS_VALUE = "all_users_value";
  public static final String ATTRIBUTE_FILTER_REGEX = "value_filter_regex";
  public static final String AND_OP_QPARSER = "andQParser";
  public static final String EXTRA_OPTS = "extra_opts";

  private List<FieldToAttributeMapping> fieldAttributeMappings = new LinkedList<>();

  private String andQParserName;
  private UserAttributeSource userAttributeSource;
  private boolean enabled = false;

@SuppressWarnings({"rawtypes"})
  @Override
  public void init(NamedList args) {
    LOG.debug("Initializing {}", this.getClass().getSimpleName());

    SolrParams solrParams = args.toSolrParams();
    if (args.getBooleanArg(ENABLED_PROP) != null) {
      this.enabled = args.getBooleanArg(ENABLED_PROP);
    }

    NamedList mappings = checkAndGet(args, FIELD_ATTR_MAPPINGS);

    Iterator<Map.Entry<String, NamedList>> iter = mappings.iterator();
    while (iter.hasNext()) {
      Map.Entry<String, NamedList> entry = iter.next();
      String solrFieldName  = entry.getKey();
      String attributeNames = checkAndGet(entry.getValue(), ATTR_NAMES);
      String filterType     = checkAndGet(entry.getValue(), FIELD_FILTER_TYPE);
      boolean acceptEmpty = false;
      if (entry.getValue().getBooleanArg(PERMIT_EMPTY_VALUES) != null) {
        acceptEmpty = entry.getValue().getBooleanArg(PERMIT_EMPTY_VALUES);
      }
      String allUsersValue  = getWithDefault(entry.getValue(), ALL_USERS_VALUE, "");
      String regex          = getWithDefault(entry.getValue(), ATTRIBUTE_FILTER_REGEX, "");
      String extraOpts      = getWithDefault(entry.getValue(), EXTRA_OPTS, "");
      FieldToAttributeMapping mapping = new FieldToAttributeMapping(solrFieldName, attributeNames, filterType, acceptEmpty, allUsersValue, regex, extraOpts);
      fieldAttributeMappings.add(mapping);
    }

    if (this.userAttributeSource == null) {
      if (solrParams.getBool(CACHE_ENABLED_PROP, CACHE_ENABLED_DEFAULT)) {
        this.userAttributeSource = new CachingUserAttributeSource(buildUserAttributeSource(solrParams), solrParams.getLong(CACHE_TTL_PROP, CACHE_TTL_DEFAULT), solrParams.getLong(CACHE_MAX_SIZE_PROP, CACHE_MAX_SIZE_DEFAULT));
      } else {
        this.userAttributeSource = buildUserAttributeSource(solrParams);
      }
    }

    this.andQParserName = this.<String>checkAndGet(args, AND_OP_QPARSER).trim();
  }

  private UserAttributeSource buildUserAttributeSource(SolrParams solrParams) {
    List<String> combinedAttributes = new LinkedList<>();
    for (FieldToAttributeMapping mapping: fieldAttributeMappings) {
      combinedAttributes.addAll(mapping.getAttributes());
    }

    String userAttributeSourceClassname = solrParams.get(USER_ATTRIBUTE_SOURCE_CLASSNAME, USER_ATTRIBUTE_SOURCE_CLASSNAME_DEFAULT);
    try {
      Class userAttributeSoureClass = Class.forName(userAttributeSourceClassname);

      UserAttributeSource attributeSource = (UserAttributeSource) userAttributeSoureClass.newInstance();

      Class<? extends UserAttributeSourceParams> attributeSourceParamsClass = attributeSource.getParamsClass();

      UserAttributeSourceParams uaParams = attributeSourceParamsClass.newInstance();
      uaParams.init(solrParams);

      attributeSource.init(uaParams, combinedAttributes);

      return attributeSource;

    } catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "User Attribute Source Class misconfigured", e);
    }

  }

  @SuppressWarnings("rawtypes")
  @Override
  public void prepare(ResponseBuilder rb, String userName) throws IOException {
    ModifiableSolrParams params = new ModifiableSolrParams(rb.req.getParams());

    Multimap<String, String> userAttributes = userAttributeSource.getAttributesForUser(userName);
    for (FieldToAttributeMapping mapping: fieldAttributeMappings) {
      String filterQuery = buildFilterQueryString(userAttributes, mapping);
      LOG.debug("Adding filter clause : {}", filterQuery);
      params.add("fq", filterQuery);
    }

    rb.req.setParams(params);

  }

  private String buildFilterQueryString(Multimap<String, String> userAttributes, FieldToAttributeMapping mapping) {
    String fieldName = mapping.getFieldName();
    Collection<String> attributeValues = getUserAttributesForField(userAttributes, mapping);
    switch (mapping.getFilterType()) {
      case OR:
        return buildSimpleORFilterQuery(fieldName, attributeValues, mapping.getAcceptEmpty(), mapping.getAllUsersValue(), mapping.getExtraOpts());
      case AND:
        return buildSubsetFilterQuery(fieldName, attributeValues, mapping.getAcceptEmpty(), mapping.getAllUsersValue(), mapping.getExtraOpts());
      case GTE:
        return buildGreaterThanFilterQuery(fieldName, attributeValues, mapping.getAcceptEmpty(), mapping.getAllUsersValue(), mapping.getExtraOpts());
      case LTE:
        return buildLessThanFilterQuery(fieldName, attributeValues, mapping.getAcceptEmpty(), mapping.getAllUsersValue(), mapping.getExtraOpts());
      default:
        return null;
    }
  }

  @VisibleForTesting
  static Collection<String> getUserAttributesForField(Multimap<String, String> userAttributes, FieldToAttributeMapping mapping) {
    Set<String> userAttributesSubset = new HashSet<>();
    Pattern regex = mapping.getAttrValueRegex();
    for (String attributeName : mapping.getAttributes()) {
      // If there is a regex to apply, we'll apply it to each element and add each element individually
      // If there isn't, we'll add the required attributes in bulk
      if (regex != null && regex.pattern().length() > 0) {
        for (String value : userAttributes.get(attributeName)) {
          String group = null;
          Matcher matcher = regex.matcher(value);
          // We're allowing Regex groups to extract specific values from the returned value
          // for example extracting common names out of a distinguished name
          // As an assumption, we're going to pull the last not-null value from the matcher and use that
          if (matcher.find()) {
            for (int i = matcher.groupCount(); i >= 0; i--) {
              group = matcher.group(i);
              if (group != null) {
                break;
              }
            }
          }
          if (group != null) {
            userAttributesSubset.add(group);
          }
        }
      } else {
          userAttributesSubset.addAll(userAttributes.get(attributeName));
      }
    }
    return userAttributesSubset;
  }

  private String buildSimpleORFilterQuery(String fieldName, Collection<String> attributeValues, boolean allowEmptyField, String allUsersValue, String extraOpts) {
    StringBuilder s = new StringBuilder();
    for (String attributeValue : attributeValues) {
      s.append(fieldName).append(":\"").append(attributeValue).append("\" ");
    }
    if (allUsersValue != null && !allUsersValue.equals("")) {
        s.append(fieldName).append(":\"").append(allUsersValue).append("\" ");
    }
    if (allowEmptyField) {
      s.append("(*:* AND -").append(fieldName).append(":*) ");
    }
    if (extraOpts != null && !extraOpts.equals("")) {
      s.append(extraOpts + " ");
    }
    s.deleteCharAt(s.length() - 1);
    return s.toString();
  }

  private String buildSubsetFilterQuery(String fieldName, Collection<String> attributeValues, boolean allowEmptyField, String allUsersValue, String extraOpts) {
    StringBuilder s = new StringBuilder();
    s.append("{!").append(andQParserName)
        .append(" set_field=").append(fieldName)
        .append(" set_value=").append(Joiner.on(',').join(attributeValues));
    if (allUsersValue != null && !allUsersValue.equals("")) {
        s.append(" wildcard_token=").append(allUsersValue);
    }
    if (allowEmptyField) {
      s.append(" allow_missing_val=true");
    } else {
      s.append(" allow_missing_val=false");
    }
    if (extraOpts != null && !extraOpts.equals("")) {
      s.append(" " + extraOpts);
    }
    s.append("}");
    return s.toString();
  }

  private String buildGreaterThanFilterQuery(String fieldName, Collection<String> attributeValues, boolean allowEmptyField, String allUsersValue, String extraOpts) {
    String value;
    if (attributeValues.size() == 1) {
        value = attributeValues.iterator().next();
    } else if (allUsersValue != null && !allUsersValue.equals("")) {
        value = allUsersValue;
    } else {
        throw new IllegalArgumentException("Greater Than Filter Query cannot be built for field " + fieldName);
    }
    StringBuilder extraClause = new StringBuilder();
    if (allowEmptyField) {
        extraClause.append(" (*:* AND -").append(fieldName).append(":*)");
    }
    if (extraOpts != null && !extraOpts.equals("")) {
      extraClause.append(" ").append(extraOpts);
    }
    return fieldName + ":[" + value + " TO *]" + extraClause.toString();
  }

  private String buildLessThanFilterQuery(String fieldName, Collection<String> attributeValues, boolean allowEmptyField, String allUsersValue, String extraOpts) {
    String value;
    if (attributeValues.size() == 1) {
        value = attributeValues.iterator().next();
    } else if (allUsersValue != null && !allUsersValue.equals("")) {
        value = allUsersValue;
    } else {
        throw new IllegalArgumentException("Less Than Filter Query cannot be built for field " + fieldName);
    }
    StringBuilder extraClause = new StringBuilder();
    if (allowEmptyField) {
      extraClause.append(" (*:* AND -").append(fieldName).append(":*)");
    }
    if (extraOpts != null && !extraOpts.equals("")) {
      extraClause.append(" ").append(extraOpts);
    }
    return fieldName + ":[* TO " + value + "]" + extraClause.toString();
  }

  @Override
  public void process(ResponseBuilder rb) throws IOException {
  }

  @Override
  public String getDescription() {
    return "Handle Query Document Authorization based on user attributes";
  }

  public String getSource() {
    return "$URL$";
  }

  @SuppressWarnings({"unchecked"})
  private <T> T checkAndGet(NamedList args, String key) {
    return (T) Preconditions.checkNotNull(args.get(key));
  }

  private <T> T getWithDefault(NamedList args, String key, T defaultValue) {
    T value = (T) args.get(key);
    if (value == null) {
      return defaultValue;
    } else {
      return value;
    }
  }

  @Override
  public boolean getEnabled() {
    return enabled;
  }
}
