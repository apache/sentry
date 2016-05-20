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

import java.io.File;
import java.util.HashSet;
import java.util.Set;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.kerberos.KerberosTicket;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

public class SentryKerberosContext implements Runnable {
  private static final float TICKET_RENEW_WINDOW = 0.80f;
  private static final Logger LOGGER = LoggerFactory
      .getLogger(SentryKerberosContext.class);
  private LoginContext loginContext;
  private Subject subject;
  private final javax.security.auth.login.Configuration kerberosConfig;
  @Deprecated
  private Thread renewerThread;
  @Deprecated
  private boolean shutDownRenewer = false;

  public SentryKerberosContext(String principal, String keyTab, boolean autoRenewTicket)
      throws LoginException {
    subject = new Subject(false, Sets.newHashSet(new KerberosPrincipal(principal)),
          new HashSet<Object>(), new HashSet<Object>());
    kerberosConfig = KerberosConfiguration.createClientConfig(principal, new File(keyTab));
    loginWithNewContext();
    if (autoRenewTicket) {
      startRenewerThread();
    }
  }

  private void loginWithNewContext() throws LoginException {
    LOGGER.info("Logging in with new Context");
    logoutSubject();
    loginContext = new LoginContext("", subject, null, kerberosConfig);
    loginContext.login();
    subject = loginContext.getSubject();
  }
  
  private void logoutSubject() {
    if (loginContext != null) {
      try {
        loginContext.logout();
      } catch (LoginException e) {
        LOGGER.warn("Error logging out the subject", e);
      }        
    }
    loginContext = null;
  }
  
  public Subject getSubject() {
    return subject;
  }

  /**
   * Get the Kerberos TGT
   * @return the user's TGT or null if none was found
   */
  @Deprecated
  private KerberosTicket getTGT() {
    Set<KerberosTicket> tickets = subject.getPrivateCredentials(KerberosTicket.class);
    for(KerberosTicket ticket: tickets) {
      KerberosPrincipal server = ticket.getServer();
      if (server.getName().equals("krbtgt/" + server.getRealm() +
          "@" + server.getRealm())) {
        return ticket;
      }
    }
    return null;
  }

  @Deprecated
  private long getRefreshTime(KerberosTicket tgt) {
    long start = tgt.getStartTime().getTime();
    long end = tgt.getEndTime().getTime();
    LOGGER.debug("Ticket start time: " + start);
    LOGGER.debug("Ticket End time: " + end);
    return start + (long) ((end - start) * TICKET_RENEW_WINDOW);
  }

  /***
   * Ticket renewer thread
   * wait till 80% time interval left on the ticket and then renew it
   */
  @Deprecated
  @Override
  public void run() {
    try {
      LOGGER.info("Sentry Ticket renewer thread started");
      while (!shutDownRenewer) {
        KerberosTicket tgt = getTGT();
        long nextRefresh = getRefreshTime(tgt);
        if (tgt == null) {
          LOGGER.warn("No ticket found in the cache");
          return;
        }
        while (System.currentTimeMillis() < nextRefresh) {
          Thread.sleep(1000);
          if (shutDownRenewer) {
            return;
          }
        }
        loginWithNewContext();
        LOGGER.debug("Renewed ticket");
      }
    } catch (InterruptedException e1) {
      LOGGER.warn("Sentry Ticket renewer thread interrupted", e1);
      return;
    } catch (LoginException e) {
      LOGGER.warn("Failed to renew ticket", e);
    } finally {
      logoutSubject();
      LOGGER.info("Sentry Ticket renewer thread finished");
    }
  }

  @Deprecated
  public void startRenewerThread() {
    renewerThread = new Thread(this);
    renewerThread.start();
  }

  public void shutDown() throws LoginException {
    if (renewerThread != null) {
      shutDownRenewer = true;
    } else {
      logoutSubject();
    }
  }
}
