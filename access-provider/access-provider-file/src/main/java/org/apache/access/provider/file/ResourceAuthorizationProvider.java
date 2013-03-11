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
package org.apache.access.provider.file;

import java.util.EnumSet;

import org.apache.access.core.AuthorizationProvider;
import org.apache.access.core.Database;
import org.apache.access.core.Privilege;
import org.apache.access.core.Server;
import org.apache.access.core.ServerResource;
import org.apache.access.core.Subject;
import org.apache.access.core.Table;
import org.apache.access.provider.file.shiro.AuthorizationOnlyIniRealm;
import org.apache.access.provider.file.shiro.UsernameToken;
import org.apache.shiro.mgt.DefaultSecurityManager;
import org.apache.shiro.mgt.SecurityManager;
import org.apache.shiro.util.ThreadContext;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

public class ResourceAuthorizationProvider implements AuthorizationProvider {

  private final AuthorizationOnlyIniRealm realm;
  private final SecurityManager securityManager;
  public ResourceAuthorizationProvider(String resource) {
    realm = new AuthorizationOnlyIniRealm(resource);
    securityManager = new DefaultSecurityManager(realm);
  }

  @Override
  public boolean hasAccess(Subject subject, Server server, Database database, Table table, EnumSet<Privilege> privileges) {
    Preconditions.checkNotNull(subject, "Subject cannot be null");
    Preconditions.checkNotNull(server, "Server cannot be null");
    Preconditions.checkNotNull(database, "Database cannot be null");
    Preconditions.checkNotNull(table, "Table cannot be null");
    Preconditions.checkNotNull(privileges, "Privileges cannot be null");
    Preconditions.checkArgument(privileges.size() > 0, "Privileges cannot be empty");
    ThreadContext.bind(securityManager);
    try {
      // TODO test to see if this is expensive
      org.apache.shiro.subject.Subject internalSubject =
          new org.apache.shiro.subject.Subject.Builder(securityManager).buildSubject();
      internalSubject.login(new UsernameToken(subject.getName()));
      return doHasAccess(internalSubject, server, database, table, privileges);
    } finally {
      ThreadContext.unbindSecurityManager();
    }
  }

  @Override
  public boolean hasAccess(Subject subject, Server server,
      ServerResource serverResource, EnumSet<Privilege> privileges) {
    Preconditions.checkNotNull(subject, "Subject cannot be null");
    Preconditions.checkNotNull(server, "Server cannot be null");
    Preconditions.checkNotNull(privileges, "Privileges cannot be null");
    Preconditions.checkArgument(privileges.size() > 0, "Privileges cannot be empty");
    ThreadContext.bind(securityManager);
    try {
      // TODO test to see if this is expensive
      org.apache.shiro.subject.Subject internalSubject =
          new org.apache.shiro.subject.Subject.Builder(securityManager).buildSubject();
      internalSubject.login(new UsernameToken(subject.getName()));
      return doHasAccess(internalSubject, server, serverResource, privileges);
    } finally {
      ThreadContext.unbindSecurityManager();
    }
  }

  private boolean doHasAccess(org.apache.shiro.subject.Subject subject, Server server, Database database,
      Table table, EnumSet<Privilege> privileges) {
    for(Privilege privilege : privileges) {
      String permission = Joiner.on(":").join(returnWildcardOrKV("server", server.getName()),
          returnWildcardOrKV("db", database.getName()),
          returnWildcardOrKV("table", table.getName()), privilege.getValue());
      if(!subject.isPermitted(permission)) {
        return false;
      }
    }
    return true;
  }

  private boolean doHasAccess(org.apache.shiro.subject.Subject subject, Server server,
      ServerResource serverResource, EnumSet<Privilege> privileges) {
    for(Privilege privilege : privileges) {
      String permission = Joiner.on(":").join(returnWildcardOrKV("server", server.getName()),
          serverResource.name().toLowerCase(), privilege.getValue());
      if(!subject.isPermitted(permission)) {
        return false;
      }
    }
    return true;
  }


  private String returnWildcardOrKV(String prefix, String value) {
    value = Strings.nullToEmpty(value).trim();
    if(value.isEmpty() || "*".equals(value)) {
      return "*";
    }
    return Joiner.on("=").join(prefix, value);
  }
}
