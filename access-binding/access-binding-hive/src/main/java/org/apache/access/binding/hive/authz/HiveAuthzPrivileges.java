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
package org.apache.access.binding.hive.authz;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

import org.apache.access.core.Action;
import org.apache.hadoop.hive.ql.hooks.Entity;

/**
 * Hive objects with required access privileges mapped to auth provider privileges
 */
public class HiveAuthzPrivileges {

  /**
   * Objects types to be authorized.
   */
  public static enum HiveObjectTypes {
    UNKNOWN,
    SERVER,
    DATABASE,
    TABLE,
    VIEW,
    URI;

    // Covert the Entity type captured by compiler to Hive Authz object type
    public static HiveObjectTypes convertHiveEntity(Entity.Type hiveEntity) {
      switch (hiveEntity) {
      case TABLE :
      case PARTITION:
        return HiveObjectTypes.TABLE;
      case DFS_DIR:
      case LOCAL_DIR:
        return HiveObjectTypes.URI;
      default:
        throw new UnsupportedOperationException("Unsupported entity type " +
            hiveEntity.name());
      }
    }
  };

  /**
   * Operation type used for privilege granting
   */
  public static enum HiveOperationType {
    UNKNOWN,
    DDL,
    DML,
    DATA_LOAD,
    DATA_UNLOAD,
    QUERY,
    INFO
  };

  /**
   * scope of the operation. The auth provider interface has different methods
   * for some of these. Hence we want to be able to identity the auth scope of
   * a statement eg. server level or DB level etc.
   */
  public static enum HiveOperationScope {
    UNKNOWN,
    SERVER,
    DATABASE,
    TABLE,
    URI
  }

  public static class AuthzPrivilegeBuilder {
    private final Map<HiveObjectTypes,EnumSet<Action>> inputPrivileges =
        new HashMap<HiveObjectTypes,EnumSet<Action>>();
    private final Map<HiveObjectTypes,EnumSet<Action>> outputPrivileges =
        new HashMap<HiveObjectTypes,EnumSet<Action>>();
    private HiveOperationType operationType;
    private HiveOperationScope operationScope;

    public AuthzPrivilegeBuilder addInputEntityPriviledge(Entity.Type inputEntityType, EnumSet<Action> inputPrivilege) {
      inputPrivileges.put(HiveObjectTypes.convertHiveEntity(inputEntityType), inputPrivilege);
      return this;
    }

    public AuthzPrivilegeBuilder addInputObjectPriviledge(HiveObjectTypes inputObjectType, EnumSet<Action> inputPrivilege) {
      inputPrivileges.put(inputObjectType, inputPrivilege);
      return this;
    }

    public AuthzPrivilegeBuilder addOutputEntityPriviledge(Entity.Type outputEntityType, EnumSet<Action> outputPrivilege) {
      outputPrivileges.put(HiveObjectTypes.convertHiveEntity(outputEntityType), outputPrivilege);
      return this;
    }

    public AuthzPrivilegeBuilder addOutputObjectPriviledge(HiveObjectTypes outputObjectType, EnumSet<Action> outputPrivilege) {
      outputPrivileges.put(outputObjectType, outputPrivilege);
      return this;
    }

    public AuthzPrivilegeBuilder setOperationType(HiveOperationType operationType) {
      this.operationType = operationType;
      return this;
    }

    public AuthzPrivilegeBuilder setOperationScope(HiveOperationScope operationScope) {
      this.operationScope = operationScope;
      return this;
    }

    public HiveAuthzPrivileges build() {
      if (operationScope.equals(HiveOperationScope.UNKNOWN)) {
        throw new UnsupportedOperationException("Operation scope is not set");
      }

      if (operationType.equals(HiveOperationType.UNKNOWN)) {
        throw new UnsupportedOperationException("Operation scope is not set");
      }

      return new HiveAuthzPrivileges(inputPrivileges, outputPrivileges, operationType, operationScope);
    }
  }

  private final Map<HiveObjectTypes,EnumSet<Action>> inputPrivileges =
      new HashMap<HiveObjectTypes,EnumSet<Action>>();
  private final Map<HiveObjectTypes,EnumSet<Action>>  outputPrivileges =
      new HashMap<HiveObjectTypes,EnumSet<Action>>();
  private final HiveOperationType operationType;
  private final HiveOperationScope operationScope;

  protected HiveAuthzPrivileges(Map<HiveObjectTypes,EnumSet<Action>> inputPrivileges,
      Map<HiveObjectTypes,EnumSet<Action>> outputPrivileges, HiveOperationType operationType,
      HiveOperationScope operationScope) {
    this.inputPrivileges.putAll(inputPrivileges);
    this.outputPrivileges.putAll(outputPrivileges);
    this.operationScope = operationScope;
    this.operationType = operationType;
  }

  /**
   * @return the inputPrivileges
   */
  public Map<HiveObjectTypes, EnumSet<Action>> getInputPrivileges() {
    return inputPrivileges;
  }

  /**
   * @return the outputPrivileges
   */
  public Map<HiveObjectTypes, EnumSet<Action>> getOutputPrivileges() {
    return outputPrivileges;
  }

  /**
   * @return the operationType
   */
  public HiveOperationType getOperationType() {
    return operationType;
  }

  /**
   * @return the operationScope
   */
  public HiveOperationScope getOperationScope() {
    return operationScope;
  }
}
