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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.sentry.policy.solr;
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNull;

import org.apache.sentry.core.model.solr.Collection;
import org.apache.sentry.core.model.solr.SolrModelAuthorizables;
import org.junit.Test;

public class TestSolrModelAuthorizables {

  @Test
  public void testCollection() throws Exception {
    Collection coll = (Collection) SolrModelAuthorizables.from("CoLleCtiOn=collection1");
    assertEquals("collection1", coll.getName());
  }

  @Test(expected=IllegalArgumentException.class)
  public void testNoKV() throws Exception {
    System.out.println(SolrModelAuthorizables.from("nonsense"));
  }

  @Test(expected=IllegalArgumentException.class)
  public void testEmptyKey() throws Exception {
    System.out.println(SolrModelAuthorizables.from("=v"));
  }

  @Test(expected=IllegalArgumentException.class)
  public void testEmptyValue() throws Exception {
    System.out.println(SolrModelAuthorizables.from("k="));
  }

  @Test
  public void testNotAuthorizable() throws Exception {
    assertNull(SolrModelAuthorizables.from("k=v"));
  }
}
