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
package org.apache.sentry.policy.indexer;

import org.apache.sentry.core.common.SentryConfigurationException;
import org.apache.sentry.core.model.indexer.Indexer;
import org.apache.sentry.core.model.indexer.IndexerModelAuthorizable;
import org.apache.sentry.policy.common.PrivilegeValidatorContext;

public class IndexerRequiredInPrivilege extends AbstractIndexerPrivilegeValidator {

  @Override
  public void validate(PrivilegeValidatorContext context) throws SentryConfigurationException {
    String privilege = context.getPrivilege();
    Iterable<IndexerModelAuthorizable> authorizables = parsePrivilege(privilege);
    boolean foundIndexerInAuthorizables = false;

    for(IndexerModelAuthorizable authorizable : authorizables) {
      if(authorizable instanceof Indexer) {
        foundIndexerInAuthorizables = true;
        break;
      }
    }
    if(!foundIndexerInAuthorizables) {
      String msg = "Missing indexer object in " + privilege;
      throw new SentryConfigurationException(msg);
    }
  }
}
