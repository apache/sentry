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
package org.apache.sentry.core.model.sqoop;

import com.google.common.collect.ImmutableList;
import org.apache.sentry.core.common.BitFieldActionFactory;
import org.apache.sentry.core.common.ImplyMethodType;
import org.apache.sentry.core.common.Model;
import org.apache.sentry.core.common.validator.PrivilegeValidator;
import org.apache.sentry.core.model.sqoop.validator.ServerNameRequiredMatch;

import java.util.HashMap;
import java.util.Map;

public class SqoopPrivilegeModel implements Model {

  private Map<String, ImplyMethodType> implyMethodMap;
  private BitFieldActionFactory bitFieldActionFactory;
  private static SqoopPrivilegeModel sqoopPrivilegeModel = new SqoopPrivilegeModel();

  private SqoopPrivilegeModel() {
    implyMethodMap = new HashMap<String, ImplyMethodType>();
    bitFieldActionFactory = new SqoopActionFactory();

    implyMethodMap.put(SqoopAuthorizable.AuthorizableType.SERVER.name().toLowerCase(), ImplyMethodType.STRING);
    implyMethodMap.put(SqoopAuthorizable.AuthorizableType.CONNECTOR.name().toLowerCase(), ImplyMethodType.STRING);
    implyMethodMap.put(SqoopAuthorizable.AuthorizableType.LINK.name().toLowerCase(), ImplyMethodType.STRING);
    implyMethodMap.put(SqoopAuthorizable.AuthorizableType.JOB.name().toLowerCase(), ImplyMethodType.STRING);
  }

  @Override
  public Map<String, ImplyMethodType> getImplyMethodMap() {
    return implyMethodMap;
  }

  @Override
  public BitFieldActionFactory getBitFieldActionFactory() {
    return bitFieldActionFactory;
  }

  public static SqoopPrivilegeModel getInstance() {
    return sqoopPrivilegeModel;
  }

  public ImmutableList<PrivilegeValidator> getPrivilegeValidators(String sqoopServerName) {
    return ImmutableList.<PrivilegeValidator>of(new ServerNameRequiredMatch(sqoopServerName));
  }

}