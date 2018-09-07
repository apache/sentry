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

/**
 * <p>
 *   Components to implement a Service Provider Interface implementation for Sentry.
 * </p>
 * <p>
 *   These classes are a way to simplify loading of different "plugin" service implementations
 * within the sentry.  It allows for the easy creation of separate <strong>Services</strong> and
 * allows for multiple implementations of that service to be defined and easily loaded.  It is able
 * to get all implementations or a single one depending on configuration and need.
 * </p>
 * <p>
 *   The Sentry API module takes advantage of the
 * <a href="https://docs.oracle.com/javase/tutorial/ext/basics/spi.html">Java Service Loader</a>
 * which was introduced in Java 7.  It uses the Factory pattern in order to get concrete instances
 * of Services and allow for custom initialization of those concrete instances.
 * </p>
 *
 * <h2>Create an Service Instance:</h2>
 *    <dl>
 *      <dt>Create concert classes for the Services Provider and ProviderFactory interfaces.</dt>
 *      <dd>
 *        <p>In order to create a service instance you need to create instances of the services
 *        Provider and ProviderFactory interfaces.  These should contain all of the necessary
 *        functions defined by the interface.</p>
 *
 *        <p>The ProviderFactory instance needs to have the getId() functioned defined to return a unique
 *        name for the Service Provider instance so that it can be looked up by that name.</p>
 *
 *        <p>The create() function of the ProviderFactory will return a concert instance of the Provider</p>
 *   <strong>Sample src/main/resources/META-INF/services/org.apache.sentry.spi.FooSomeProvider file</strong>
 *     <pre>{@code
 *   package org.apache.sentry.fake;
 *
 *   public class FooSomeProvider implements SomeProvider {
 *     public void doSomething(){
 *       ... does something ...
 *     }
 *   }
 *   }</pre>
 *
 *   <strong>Sample src/main/resources/META-INF/services/org.apache.sentry.spi.FooSomeProviderFactory file</strong>
 *     <pre>{@code
 *   package org.apache.sentry.fake;
 *
 *   public class FooSomeProviderFactory implements SomeProviderFactory {
 *     @Override
 *     public String getId() {
 *        return "foo"
 *     }
 *
 *     @Override
 *     public SomeProvider create() {
 *       return new SomeProvider();
 *     }
 *   }
 *   }</pre>
 *      </dd>
 *      <dt>Create an entry for the ProviderFactory instance in the service configuration file for
 *      the SPI</dt>
 *      <dd>
 *        <p>The service configuration file is placed in the META-INF/services directory of a jar and
 *        is named after the ProviderFactory instance of the SPI.</p>
 *       <strong>Sample src/main/resources/META-INF/services/org.apache.sentry.fake.SomeProviderFactory file</strong>
 *       <pre>{@code
 *       org.apache.sentry.fake.FooSomeProviderFactory
 *       }</pre>
 *      </dd>
 *      <dt>Load the Service instance with the ProviderManager</dt>
 *      <dd>
 *        <p>You can then load the service from code with the ProviderManager.  Either all service
 *        providers or an individual one.</p>
 *       <pre>{@code
 *       # Loads instances of all SomeProviderFactory defined
 *       List<SomeProviderFactory> = ProviderManager.getInstance().load("some-spi");
 *
 *       # Loads only the "foo" instance of the SomeProviderFactory
 *       SomeProviderFactory someProviderFactory = ProviderManager.getInstance().load("some-spi", "foo");
 *       }</pre>
 *      </dd>
 *
 *
 *
 * <h2>How to Implement a New Service:</h2>
 * <dl>
 *   <dt>Create an SPI implantation</dt>
 *   <dd>
 *     <p>
 *       You can create Service by implementing a concrete instance of the SPI interface.
 *       This interface will provider information about what interfaces the SPI will be looking for
 *       when loading instances.  It requires the Provider and the ProviderFactory information as
 *       well as a unique name for the Service in the system.
 *   </p>
 *   <strong>Sample src/main/java/org/apache/sentry/fake/SomeSpi.java file</strong>
 *   <pre>{@code
 *   package org.apache.sentry.fake;
 *
 *   public class SomeSpi implements Spi {
 *
 *   @Override
 *   public String getName() {
 *     return "some-spi";
 *   }
 *
 *   @Override
 *   public Class<? extends Provider> getProviderClass() {
 *     return SomeProvider.class;
 *   }
 *
 *   @Override
 *   public Class<? extends ProviderFactory> getProviderFactoryClass() {
 *     return SomeProviderFactory.class;
 *   }
 * }
 *   }</pre>
 *   <p>
 *      As well you must put an entry for the SPI concrete class in the
 *      <strong>META-INF/services/org.apache.sentry.spi.Spi</strong> service configuration file pointing to that instance.
 *   </p>
 *   <strong>Sample src/main/resources/META-INF/services/org.apache.sentry.spi.SomeSpi file</strong>
 *   <pre>{@code
 *   org.apache.sentry.fake.SomeSpi
 *   org.apache.sentry.fake.SomeOtherSpi
 *   }</pre>
 *   <dt>Create a the Provider and Provider Factory interfaces</dt>
 *   <dd>
 *     <p>You need to create the interfaces referenced in the the SPI class.  These extend the
 *     Provider and ProviderFactory interfaces and can be customized to have the function definitions
 *     for how you want your service to operate.</p>
 *
 *   <strong>Sample rc/main/java/org/apache/sentry/fake/SomeProvider.java file</strong>
 *     <pre>{@code
 *   package org.apache.sentry.fake;
 *
 *   public interface SomeProvider extends Provider {
 *     void doSomething();
 *   }
 *
 *   }</pre>
 *   <strong>Sample src/main/java/org/apache/sentry/fake/SomeProviderFactory.java file</strong>
 *   <pre>{@code
 *   package org.apache.sentry.fake;
 *
 *   public interface SomeProviderFactory extends ProviderFactory<SomeProvider> {
 *      void init(SomeConfig  config);
 *   }
 *   }</pre>
 *
 *   </dd>
 * </dl>
 *
 *
 *
 */
package org.apache.sentry.spi;
