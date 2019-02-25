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
package org.apache.solr.handler.component;

import com.google.common.collect.Multimap;

import java.util.Collection;

/**
 * Interface for a source for user attributes. Instances are configurable by an implementation
 * of {@link UserAttributeSourceParams} which can be obtained via getParamsClass.
 */
public interface UserAttributeSource {
    /**
     * @param userName
     * @return Multimap of attributes for the given user in the form attributeName->Set(values)
     */
    Multimap<String, String> getAttributesForUser(String userName);

    /**
     * @return The implementation of {@link UserAttributeSourceParams} that is used to configure
     * this class.
     */
    Class<? extends UserAttributeSourceParams> getParamsClass();

    /**
     * @param params An instance of {@link UserAttributeSourceParams} to configure this class
     * @param attributes Specifies the possible attributes that will be returned as part of the search
     */
    void init(UserAttributeSourceParams params, Collection<String> attributes);
}
