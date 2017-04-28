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

import com.google.common.net.HostAndPort;
import org.apache.sentry.core.common.utils.ThriftUtil;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestPoolClientInvocationHandler {
  private static final Logger LOGGER =
    LoggerFactory.getLogger(TestPoolClientInvocationHandler.class);

  private void expectParseHostPortStrings(String hostsAndPortsStr,
        String[] expectedHosts, int[] expectedPorts) throws Exception {
    boolean success = false;
    String[] hostsAndPortsStrArr = hostsAndPortsStr.split(",");
    HostAndPort[] hostsAndPorts;
    try {
      hostsAndPorts = ThriftUtil.parseHostPortStrings(hostsAndPortsStrArr, 8038);
      success = true;
    } finally {
      if (!success) {
        LOGGER.error("Caught exception while parsing hosts/ports string " +
            hostsAndPortsStr);
      }
    }
    String[] hosts = new String[hostsAndPortsStrArr.length];
    int[] ports = new int[hostsAndPortsStrArr.length];
    parseHostsAndPorts(hostsAndPorts, hosts, ports);
    Assert.assertArrayEquals("Got unexpected hosts results while " +
        "parsing " + hostsAndPortsStr, expectedHosts, hosts);
    Assert.assertArrayEquals("Got unexpected ports results while " +
        "parsing " + hostsAndPortsStr, expectedPorts, ports);
  }

  private void parseHostsAndPorts(HostAndPort[] hostsAndPorts, String[] hosts, int[] ports) {
    for (int i = 0; i < hostsAndPorts.length; i++) {
      hosts[i] = hostsAndPorts[i].getHostText();
      ports[i] = hostsAndPorts[i].getPort();
    }
  }

  @SuppressWarnings("PMD.AvoidUsingHardCodedIP")
  @Test
  public void testParseHostPortStrings() throws Exception {
    expectParseHostPortStrings("foo", new String[] {"foo"}, new int[] {8038});
    expectParseHostPortStrings("foo,bar",
        new String[] {"foo", "bar"},
        new int[] {8038, 8038});
    expectParseHostPortStrings("foo:2020,bar:2021",
        new String[] {"foo", "bar"},
        new int[] {2020, 2021});
    expectParseHostPortStrings("127.0.0.1:2020,127.1.0.1",
        new String[] {"127.0.0.1", "127.1.0.1"},
        new int[] {2020, 8038});
    expectParseHostPortStrings("[2001:db8:85a3:8d3:1319:8a2e:370:7348]:433",
        new String[] {"2001:db8:85a3:8d3:1319:8a2e:370:7348"},
        new int[] {433});
  }
}
