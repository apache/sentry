/*
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
 *
 */

package org.apache.sentry.spi;

import java.util.List;
import org.junit.Assert;
import org.junit.Test;

/**
 * Testing of the SPI and Provider mechanisms
 */
public class TestProviderManager {

  @Test
  public void testLoadingAll() {
    List<SomeTestProviderFactory> factories = ProviderManager.getInstance().load(SomeTestSpi.SPI);
    Assert.assertEquals("Wrong count of service providers found", 2, factories.size());
    Assert.assertEquals("Missing instance of A", 1,
        factories.stream().filter(spi -> spi instanceof SomeTestProviderImplA).count());
    Assert.assertEquals("Missing instance of B", 1,
        factories.stream().filter(spi -> spi instanceof SomeTestProviderImplB).count());
    Assert.assertEquals("Instance loaded that should not be.", 0,
        factories.stream().filter(spi -> spi instanceof SomeTestProviderImplNotLoaded).count());
  }

  @Test(expected = RuntimeException.class)
  public void testInvalidSPI() {
    ProviderManager.getInstance().load("NotThere");
  }

  @Test
  public void testLoadingSingle() {
    SomeTestProviderFactory factory = ProviderManager.getInstance().load(SomeTestSpi.SPI, "A");
    Assert.assertNotNull("Service provider should have been found", factory);
    Assert.assertEquals("Wrong service provider loaded", "A", factory.getId());
  }

  @Test
  public void testLoadingMissingSingle() {
    SomeTestProviderFactory factory = ProviderManager.getInstance().load(SomeTestSpi.SPI, "Nope");
    Assert.assertNull("Service provider should not have been found", factory);
  }
}
