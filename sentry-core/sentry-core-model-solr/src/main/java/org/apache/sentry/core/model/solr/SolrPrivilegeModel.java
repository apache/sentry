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
package org.apache.sentry.core.model.solr;

import com.google.common.collect.ImmutableList;
import org.apache.sentry.core.common.BitFieldActionFactory;
import org.apache.sentry.core.common.ImplyMethodType;
import org.apache.sentry.core.common.Model;
import org.apache.sentry.core.common.validator.PrivilegeValidator;
import org.apache.sentry.core.model.solr.validator.SolrPrivilegeValidator;

import java.util.HashMap;
import java.util.Map;

/**
 * This class provides the concrete implementation of Sentry authorization model
 * for Apache Solr.
 */
public class SolrPrivilegeModel implements Model {

  private Map<String, ImplyMethodType> implyMethodMap;
  private BitFieldActionFactory bitFieldActionFactory;
  private static SolrPrivilegeModel solrPrivilegeModel = new SolrPrivilegeModel();

  private SolrPrivilegeModel() {
    implyMethodMap = new HashMap<String, ImplyMethodType>();
    bitFieldActionFactory = new SolrActionFactory();

    implyMethodMap.put(SolrModelAuthorizable.AuthorizableType.Collection.name().toLowerCase(), ImplyMethodType.STRING);
    implyMethodMap.put(SolrModelAuthorizable.AuthorizableType.Config.name().toLowerCase(), ImplyMethodType.STRING);
    implyMethodMap.put(SolrModelAuthorizable.AuthorizableType.Schema.name().toLowerCase(), ImplyMethodType.STRING);
    implyMethodMap.put(SolrModelAuthorizable.AuthorizableType.Admin.name().toLowerCase(), ImplyMethodType.STRING);
  }

  @Override
  public Map<String, ImplyMethodType> getImplyMethodMap() {
    return implyMethodMap;
  }

  @Override
  public BitFieldActionFactory getBitFieldActionFactory() {
    return bitFieldActionFactory;
  }

  public static SolrPrivilegeModel getInstance() {
    return solrPrivilegeModel;
  }

  public ImmutableList<PrivilegeValidator> getPrivilegeValidators() {
    return ImmutableList.<PrivilegeValidator>of(new SolrPrivilegeValidator());
  }
}