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
package org.apache.sentry.core.model.db;

import com.google.common.collect.ImmutableList;
import org.apache.sentry.core.common.BitFieldActionFactory;
import org.apache.sentry.core.common.ImplyMethodType;
import org.apache.sentry.core.common.Model;
import org.apache.sentry.core.common.validator.PrivilegeValidator;
import org.apache.sentry.core.model.db.validator.DatabaseMustMatch;
import org.apache.sentry.core.model.db.validator.DatabaseRequiredInPrivilege;
import org.apache.sentry.core.model.db.validator.ServerNameMustMatch;
import org.apache.sentry.core.model.db.validator.ServersAllIsInvalid;

import java.util.HashMap;
import java.util.Map;

import static org.apache.sentry.core.model.db.DBModelAuthorizable.AuthorizableType;

public class HivePrivilegeModel implements Model {
  private Map<String, ImplyMethodType> implyMethodMap;
  private BitFieldActionFactory bitFieldActionFactory;
  private static HivePrivilegeModel hivePrivilegeModel = new HivePrivilegeModel();

  private HivePrivilegeModel() {
    implyMethodMap = new HashMap<String, ImplyMethodType>();
    bitFieldActionFactory = new HiveActionFactory();

    implyMethodMap.put(AuthorizableType.Server.name().toLowerCase(), ImplyMethodType.STRING);
    implyMethodMap.put(AuthorizableType.Db.name().toLowerCase(), ImplyMethodType.STRING);
    implyMethodMap.put(AuthorizableType.Table.name().toLowerCase(), ImplyMethodType.STRING);
    implyMethodMap.put(AuthorizableType.Column.name().toLowerCase(), ImplyMethodType.STRING);
    implyMethodMap.put(AuthorizableType.URI.name().toLowerCase(), ImplyMethodType.URL);
  }

  @Override
  public Map<String, ImplyMethodType> getImplyMethodMap() {
    return implyMethodMap;
  }

  @Override
  public BitFieldActionFactory getBitFieldActionFactory() {
    return bitFieldActionFactory;
  }

  public static HivePrivilegeModel getInstance() {
    return hivePrivilegeModel;
  }

  public ImmutableList<PrivilegeValidator> getPrivilegeValidators(String serverName) {
    return ImmutableList.<PrivilegeValidator>of(new ServersAllIsInvalid(), new DatabaseMustMatch(),
            new DatabaseRequiredInPrivilege(), new ServerNameMustMatch(serverName));
  }
}