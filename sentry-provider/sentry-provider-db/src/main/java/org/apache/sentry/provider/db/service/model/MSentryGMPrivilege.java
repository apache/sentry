/**
vim  * Licensed to the Apache Software Foundation (ASF) under one
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
package org.apache.sentry.provider.db.service.model;

import static org.apache.sentry.provider.common.ProviderConstants.AUTHORIZABLE_JOINER;
import static org.apache.sentry.provider.common.ProviderConstants.KV_JOINER;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import javax.jdo.annotations.PersistenceCapable;
import org.apache.sentry.core.common.Authorizable;
import org.apache.sentry.core.model.db.AccessConstants;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;

/**
 * Database backed Sentry Generic Privilege for new authorization Model
 * Any changes to this object
 * require re-running the maven build so DN an re-enhance.
 */
@PersistenceCapable
public class MSentryGMPrivilege {
  public static final String PREFIX_RESOURCE_NAME = "resourceName";
  public static final String PREFIX_RESOURCE_TYPE = "resourceType";
  public static final int AUTHORIZABLE_LEVEL = 4;

  private static final String NULL_COL = "__NULL__";
  private static final String SERVICE_SCOPE = "Server";

  /**
   * The authorizable List has been stored into resourceName and resourceField columns
   * We assume that the generic model privilege for any component(hive/impala or solr) doesn't exceed four level.
   * This generic model privilege currently can support maximum 4 level.
   **/
  private String resourceName0 = NULL_COL;
  private String resourceType0 = NULL_COL;
  private String resourceName1 = NULL_COL;
  private String resourceType1 = NULL_COL;
  private String resourceName2 = NULL_COL;
  private String resourceType2 = NULL_COL;
  private String resourceName3 = NULL_COL;
  private String resourceType3 = NULL_COL;

  private String serviceName;
  private String componentName;
  private String action;
  private String scope;

  private Boolean grantOption = false;
  // roles this privilege is a part of
  private Set<MSentryRole> roles;
  private long createTime;

  public MSentryGMPrivilege() {
    this.roles = new HashSet<MSentryRole>();
  }

  public MSentryGMPrivilege(String componentName, String serviceName,
                                 List<? extends Authorizable> authorizables,
                                 String action, Boolean grantOption) {
    this.componentName = MSentryUtil.safeIntern(componentName);
    this.serviceName = MSentryUtil.safeIntern(serviceName);
    this.action = MSentryUtil.safeIntern(action);
    this.grantOption = grantOption;
    this.roles = new HashSet<>();
    this.createTime = System.currentTimeMillis();
    setAuthorizables(authorizables);
  }

  public MSentryGMPrivilege(MSentryGMPrivilege copy) {
    this.action = copy.action;
    this.componentName = copy.componentName;
    this.serviceName = copy.serviceName;
    this.grantOption = copy.grantOption;
    this.scope = copy.scope;
    this.createTime = copy.createTime;
    setAuthorizables(copy.getAuthorizables());
    this.roles = new HashSet<MSentryRole>();
    roles.addAll(copy.roles);
  }

  public String getServiceName() {
    return serviceName;
  }

  public void setServiceName(String serviceName) {
    this.serviceName = serviceName;
  }

  public String getComponentName() {
    return componentName;
  }

  public void setComponentName(String componentName) {
    this.componentName = componentName;
  }

  public String getAction() {
    return action;
  }

  public void setAction(String action) {
    this.action = action;
  }

  public Boolean getGrantOption() {
    return grantOption;
  }

  public void setGrantOption(Boolean grantOption) {
    this.grantOption = grantOption;
  }

  public Set<MSentryRole> getRoles() {
    return roles;
  }

  public void setRoles(Set<MSentryRole> roles) {
    this.roles = roles;
  }

  public long getCreateTime() {
    return createTime;
  }

  public void setCreateTime(long createTime) {
    this.createTime = createTime;
  }

  public String getScope() {
    return scope;
  }

  public List<? extends Authorizable> getAuthorizables() {
    List<Authorizable> authorizables = Lists.newArrayList();
    //construct atuhorizable lists
    for (int i = 0; i < AUTHORIZABLE_LEVEL; i++) {
      final String resourceName = (String) getField(this, PREFIX_RESOURCE_NAME + String.valueOf(i));
      final String resourceTYpe = (String) getField(this, PREFIX_RESOURCE_TYPE + String.valueOf(i));

      if (notNULL(resourceName) && notNULL(resourceTYpe)) {
        authorizables.add(new Authorizable() {
          @Override
          public String getTypeName() {
            return resourceTYpe;
          }
          @Override
          public String getName() {
            return resourceName;
          }
        });
      }
    }
    return authorizables;
  }

  /**
   * Only allow strict hierarchies. That is, can level =1 be not null when level = 0 is null
   * @param authorizables
   */
  public void setAuthorizables(List<? extends Authorizable> authorizables) {
    if ((authorizables == null) || (authorizables.isEmpty())) {
      //service scope
      scope = SERVICE_SCOPE;
      return;
    }
    if (authorizables.size() > AUTHORIZABLE_LEVEL) {
      throw new IllegalStateException("This generic privilege model only supports maximum 4 level.");
    }

    for (int i = 0; i < authorizables.size(); i++) {
      Authorizable authorizable = authorizables.get(i);
      if (authorizable == null) {
        String msg = String.format("The authorizable can't be null. Please check authorizables[%d]:", i);
        throw new IllegalStateException(msg);
      }
      String resourceName = authorizable.getName();
      String resourceTYpe = authorizable.getTypeName();
      if (isNULL(resourceName) || isNULL(resourceTYpe)) {
        String msg = String.format("The name and type of authorizable can't be empty or null.Please check authorizables[%d]", i);
        throw new IllegalStateException(msg);
      }
      setField(this, PREFIX_RESOURCE_NAME + String.valueOf(i), toNULLCol(resourceName));
      setField(this, PREFIX_RESOURCE_TYPE + String.valueOf(i), toNULLCol(resourceTYpe));
      scope = resourceTYpe;
    }
  }

  public void appendRole(MSentryRole role) {
    if (roles.add(role)) {
      role.appendGMPrivilege(this);
    }
  }

  public void removeRole(MSentryRole role) {
    if(roles.remove(role)) {
      role.removeGMPrivilege(this);
    }
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((action == null) ? 0 : action.hashCode());
    result = prime * result + ((componentName == null) ? 0 : componentName.hashCode());
    result = prime * result + ((serviceName == null) ? 0 : serviceName.hashCode());
    result = prime * result + ((grantOption == null) ? 0 : grantOption.hashCode());
    result = prime * result + ((scope == null) ? 0 : scope.hashCode());

    for (Authorizable authorizable : getAuthorizables()) {
      result = prime * result + authorizable.getName().hashCode();
      result = prime * result + authorizable.getTypeName().hashCode();
    }

    return result;
  }

  @Override
  public String toString() {
    List<String> unifiedNames = Lists.newArrayList();
    for (Authorizable auth : getAuthorizables()) {
      unifiedNames.add(KV_JOINER.join(auth.getTypeName(),auth.getName()));
    }

    return "MSentryGMPrivilege ["
        + "serverName=" + serviceName + ", componentName=" + componentName
        + ", authorizables=" + AUTHORIZABLE_JOINER.join(unifiedNames)+ ", scope=" + scope
        + ", action=" + action + ", roles=[...]"  + ", createTime="
        + createTime + ", grantOption=" + grantOption +"]";
  }

  @Override
  public boolean equals(Object obj) {
      if (this == obj)
          return true;
      if (obj == null)
          return false;
      if (getClass() != obj.getClass())
          return false;
      MSentryGMPrivilege other = (MSentryGMPrivilege) obj;
      if (action == null) {
          if (other.action != null)
              return false;
      } else if (!action.equalsIgnoreCase(other.action))
          return false;
      if (scope == null) {
        if (other.scope != null)
            return false;
      } else if (!scope.equals(other.scope))
        return false;
      if (serviceName == null) {
          if (other.serviceName != null)
              return false;
      } else if (!serviceName.equals(other.serviceName))
          return false;
      if (componentName == null) {
          if (other.componentName != null)
              return false;
      } else if (!componentName.equals(other.componentName))
          return false;
      if (grantOption == null) {
        if (other.grantOption != null)
          return false;
      } else if (!grantOption.equals(other.grantOption))
        return false;

      List<? extends Authorizable> authorizables = getAuthorizables();
      List<? extends Authorizable> other_authorizables = other.getAuthorizables();

      if (authorizables.size() != other_authorizables.size()) {
        return false;
      }
      for (int i = 0; i < authorizables.size(); i++) {
        String o1 = KV_JOINER.join(authorizables.get(i).getTypeName(),
                                         authorizables.get(i).getName());
        String o2 = KV_JOINER.join(other_authorizables.get(i).getTypeName(),
            other_authorizables.get(i).getName());
        if (!o1.equals(o2)) {
          return false;
        }
      }
      return true;
  }

  /**
   * Return true if this privilege implies request privilege
   * Otherwise, return false
   * @param request, other privilege
   */
  public boolean implies(MSentryGMPrivilege request) {
    //component check
    if (!componentName.equals(request.getComponentName())) {
      return false;
    }
    //service check
    if (!serviceName.equals(request.getServiceName())) {
      return false;
    }
    // check action implies
    if (!action.equalsIgnoreCase(AccessConstants.ALL)
        && !action.equalsIgnoreCase(request.getAction())
        && !action.equalsIgnoreCase(AccessConstants.ACTION_ALL)) {
      return false;
    }
    //check authorizable list implies
    Iterator<? extends Authorizable> existIterator = getAuthorizables().iterator();
    Iterator<? extends Authorizable> requestIterator = request.getAuthorizables().iterator();
    while (existIterator.hasNext() && requestIterator.hasNext()) {
      Authorizable existAuth = existIterator.next();
      Authorizable requestAuth = requestIterator.next();
      //check authorizable type
      if (!existAuth.getTypeName().equals(requestAuth.getTypeName())) {
        return false;
      }
      //check authorizable name
      if (!existAuth.getName().equals(requestAuth.getName())) {
        /**The persistent authorizable isn't equal the request authorizable
        * but the following situations are pass check
        * The name of persistent authorizable is ALL or "*"
        */
        if (existAuth.getName().equalsIgnoreCase(AccessConstants.ACTION_ALL)
            || existAuth.getName().equalsIgnoreCase(AccessConstants.ALL)) {
          continue;
        } else {
          return false;
        }
      }
    }

    if ( (!existIterator.hasNext()) && (!requestIterator.hasNext()) ){
      /**
       * The persistent privilege has the same authorizables size as the requested privilege
       * The check is pass
       */
      return true;

    } else if (existIterator.hasNext()) {
      /**
       * The persistent privilege has much more authorizables than request privilege,so its scope is less
       * than the requested privilege.
       * There is a situation that the check is pass, the name of the exceeding authorizables is ALL or "*".
       * Take the Solr for example,the exist privilege is collection=c1->field=*->action=query
       * the request privilege is collection=c1->action=query, the check is pass
       */
      while (existIterator.hasNext()) {
        Authorizable existAuthorizable = existIterator.next();
        if (existAuthorizable.getName().equalsIgnoreCase(AccessConstants.ALL)
            || existAuthorizable.getName().equalsIgnoreCase(AccessConstants.ACTION_ALL)) {
          continue;
        } else {
          return false;
        }
      }
    } else {
      /**
       * The requested privilege has much more authorizables than persistent privilege, so its scope is less
       * than the persistent privilege
       * The check is pass
       */
      return true;
    }

    return true;
  }

  public static String toNULLCol(String col) {
    return Strings.isNullOrEmpty(col) ? NULL_COL : col;
  }

  public static boolean notNULL(String s) {
    return !(Strings.isNullOrEmpty(s) || NULL_COL.equals(s));
  }

  public static boolean isNULL(String s) {
    return !notNULL(s);
  }

  public static <T> void setField(Object obj, String fieldName, T fieldValue) {
    try {
      Class<?> clazz = obj.getClass();
      Field field=clazz.getDeclaredField(fieldName);
      field.setAccessible(true);
      field.set(obj, fieldValue);
    } catch (Exception e) {
      throw new RuntimeException("setField error: " + e.getMessage(), e);
    }
  }

  @SuppressWarnings("unchecked")
  public static <T> T getField(Object obj, String fieldName) {
    try {
      Class<?> clazz = obj.getClass();
      Field field=clazz.getDeclaredField(fieldName);
      field.setAccessible(true);
      return (T)field.get(obj);
    } catch (Exception e) {
      throw new RuntimeException("getField error: " + e.getMessage(), e);
    }
  }

}
