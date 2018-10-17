/**
 * Licensed to the Apache Software Foundation (ASF) under one
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
package org.apache.sentry.service.thrift;

import java.util.Arrays;
import java.util.List;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.AuthorizeCallback;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.SaslRpcServer;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.apache.hadoop.security.authentication.util.KerberosName.NoMatchingRule;
import org.apache.sentry.core.common.exception.ConnectionDeniedException;
import org.apache.sentry.service.common.ServiceConstants.ServerConfig;
import org.slf4j.LoggerFactory;

public class GSSCallback extends SaslRpcServer.SaslGssCallbackHandler {

  private final Configuration conf;
  public GSSCallback(Configuration conf) {
    super();
    this.conf = conf;
  }

  boolean comparePrincipals(String principal1, String principal2) {
    String[] principalParts1 = SaslRpcServer.splitKerberosName(principal1);
    String[] principalParts2 = SaslRpcServer.splitKerberosName(principal2);
    if (principalParts1.length == 0 || principalParts2.length == 0) {
      return false;
    }
    if (principalParts1.length == principalParts2.length) {
      for (int i=0; i < principalParts1.length; i++) {
        if (!principalParts1[i].equals(principalParts2[i])) {
          return false;
        }
      }
      return true;
    } else {
      return false;
    }
  }

  boolean allowConnect(String principal) {
    String allowedPrincipals = conf.get(ServerConfig.ALLOW_CONNECT);
    if (allowedPrincipals == null) {
      return false;
    }
    String principalShortName;
    if (KerberosName.hasRulesBeenSet()) {
      try {
        KerberosName krbName = new KerberosName(principal);
        principalShortName = krbName.getShortName();
        //To accommodate HADOOP-12751 where some versions don't throw NoMatchingRule exception
        if (principalShortName.equals(principal)) {
          principalShortName = getShortName(principal);
        }
      } catch (NoMatchingRule e) {
        LoggerFactory.getLogger(GSSCallback.class)
            .debug("No matching rule found for principal " + principal, e);
        principalShortName = getShortName(principal);
      } catch (Exception e) {
        LoggerFactory.getLogger(GSSCallback.class)
            .debug("Cannot derive short name from KerberosName. "
                + "Use principal name prefix to authenticate", e);
        principalShortName = getShortName(principal);
      }

    } else {
      principalShortName = getShortName(principal);
    }

    List<String> items = Arrays.asList(allowedPrincipals.split("\\s*,\\s*"));
    for (String item : items) {
      if (comparePrincipals(item, principalShortName)) {
        return true;
      }
    }
    return false;
  }

  private String getShortName(String principal) {
    String parts[] = SaslRpcServer.splitKerberosName(principal);
    return parts[0];
  }

  @Override
  public void handle(Callback[] callbacks)
  throws UnsupportedCallbackException, ConnectionDeniedException {
    AuthorizeCallback ac = null;
    for (Callback callback : callbacks) {
      if (callback instanceof AuthorizeCallback) {
        ac = (AuthorizeCallback) callback;
      } else {
        throw new UnsupportedCallbackException(callback,
            "Unrecognized SASL GSSAPI Callback");
      }
    }
    if (ac != null) {
      String authid = ac.getAuthenticationID();
      String authzid = ac.getAuthorizationID();

      if (allowConnect(authid)) {
        if (authid.equals(authzid)) {
          ac.setAuthorized(true);
        } else {
          ac.setAuthorized(false);
        }
        if (ac.isAuthorized()) {
          ac.setAuthorizedID(authzid);
        }
      } else {
        throw new ConnectionDeniedException(ac,
            "Connection to sentry service denied due to lack of client credentials",
            authid);
      }
    }
  }
}