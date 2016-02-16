/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sentry.kafka.binding;

import java.lang.reflect.Constructor;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.Sets;
import kafka.network.RequestChannel;
import kafka.security.auth.Operation;
import kafka.security.auth.Resource;
import org.apache.sentry.core.common.ActiveRoleSet;
import org.apache.sentry.core.common.Authorizable;
import org.apache.sentry.core.common.Subject;
import org.apache.sentry.core.model.kafka.KafkaActionFactory;
import org.apache.sentry.core.model.kafka.KafkaActionFactory.KafkaAction;
import org.apache.sentry.kafka.ConvertUtil;
import org.apache.sentry.kafka.conf.KafkaAuthConf.AuthzConfVars;
import org.apache.sentry.policy.common.PolicyEngine;
import org.apache.sentry.provider.common.AuthorizationComponent;
import org.apache.sentry.provider.common.AuthorizationProvider;
import org.apache.sentry.provider.common.ProviderBackend;
import org.apache.sentry.provider.db.generic.SentryGenericProviderBackend;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaAuthBinding {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaAuthBinding.class);
    private static final String COMPONENT_TYPE = AuthorizationComponent.KAFKA;

    private final Configuration authConf;
    private final AuthorizationProvider authProvider;
    private ProviderBackend providerBackend;

    private final KafkaActionFactory actionFactory = KafkaActionFactory.getInstance();

    public KafkaAuthBinding(Configuration authConf) throws Exception {
        this.authConf = authConf;
        this.authProvider = createAuthProvider();
    }

    /**
     * Instantiate the configured authz provider
     *
     * @return {@link AuthorizationProvider}
     */
    private AuthorizationProvider createAuthProvider() throws Exception {
        /**
         * get the authProvider class, policyEngine class, providerBackend class and resources from the
         * kafkaAuthConf config
         */
        String authProviderName =
            authConf.get(AuthzConfVars.AUTHZ_PROVIDER.getVar(),
                AuthzConfVars.AUTHZ_PROVIDER.getDefault());
        String resourceName =
            authConf.get(AuthzConfVars.AUTHZ_PROVIDER_RESOURCE.getVar(),
                AuthzConfVars.AUTHZ_PROVIDER_RESOURCE.getDefault());
        String providerBackendName =
            authConf.get(AuthzConfVars.AUTHZ_PROVIDER_BACKEND.getVar(),
                AuthzConfVars.AUTHZ_PROVIDER_BACKEND.getDefault());
        String policyEngineName =
            authConf.get(AuthzConfVars.AUTHZ_POLICY_ENGINE.getVar(),
                AuthzConfVars.AUTHZ_POLICY_ENGINE.getDefault());
        String instanceName = authConf.get(AuthzConfVars.AUTHZ_INSTANCE_NAME.getVar());
        if (resourceName != null && resourceName.startsWith("classpath:")) {
            String resourceFileName = resourceName.substring("classpath:".length());
            resourceName = AuthorizationProvider.class.getClassLoader().getResource(resourceFileName).getPath();
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Using authorization provider " + authProviderName + " with resource "
                + resourceName + ", policy engine " + policyEngineName + ", provider backend "
                + providerBackendName);
        }

        // Instantiate the configured providerBackend
        Constructor<?> providerBackendConstructor =
            Class.forName(providerBackendName)
                .getDeclaredConstructor(Configuration.class, String.class);
        providerBackendConstructor.setAccessible(true);
        providerBackend =
            (ProviderBackend) providerBackendConstructor.newInstance(new Object[]{authConf,
                resourceName});
        if (providerBackend instanceof SentryGenericProviderBackend) {
            ((SentryGenericProviderBackend) providerBackend).setComponentType(COMPONENT_TYPE);
            ((SentryGenericProviderBackend) providerBackend).setServiceName("kafka" + instanceName);
        }

        // Instantiate the configured policyEngine
        Constructor<?> policyConstructor =
            Class.forName(policyEngineName).getDeclaredConstructor(ProviderBackend.class);
        policyConstructor.setAccessible(true);
        PolicyEngine policyEngine =
            (PolicyEngine) policyConstructor.newInstance(new Object[]{providerBackend});

        // Instantiate the configured authProvider
        Constructor<?> constructor =
            Class.forName(authProviderName).getDeclaredConstructor(Configuration.class, String.class,
                PolicyEngine.class);
        constructor.setAccessible(true);
        return (AuthorizationProvider) constructor.newInstance(new Object[]{authConf, resourceName,
            policyEngine});
    }

    /**
     * Authorize access to a Kafka privilege
     */
    public boolean authorize(RequestChannel.Session session, Operation operation, Resource resource) {
        List<Authorizable> authorizables = ConvertUtil.convertResourceToAuthorizable(session.clientAddress().getHostAddress(), resource);
        Set<KafkaAction> actions = Sets.newHashSet(actionFactory.getActionByName(operation.name()));
        return authProvider.hasAccess(new Subject(getName(session)), authorizables, actions, ActiveRoleSet.ALL);
    }

    /*
    * For SSL session's Kafka creates user names with "CN=" prepended to the user name.
    * "=" is used as splitter by Sentry to parse key value pairs and so it is required to strip off "CN=".
    * */
    private String getName(RequestChannel.Session session) {
        final String principalName = session.principal().getName();
        int start = principalName.indexOf("CN=");
        if (start >= 0) {
            String tmpName, name = "";
                tmpName = principalName.substring(start + 3);
                int end = tmpName.indexOf(",");
                if (end > 0) {
                    name = tmpName.substring(0, end);
                } else {
                    name = tmpName;
                }
            return name;
        } else {
            return principalName;
        }
    }
}
