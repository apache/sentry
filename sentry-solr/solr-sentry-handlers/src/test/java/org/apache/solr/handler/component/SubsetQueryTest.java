package org.apache.solr.handler.component;
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

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test for QueryDocAuthorizationComponent (with conjunctive match) and SubsetQueryPlugin
 */
public class SubsetQueryTest extends SolrTestCaseJ4 {
  private static final String f = "stringdv";
  private static final String countField = "valcount";
  private static final String qParser = "subset";

  @BeforeClass
  public static void beforeTests() throws Exception {
    initCore("solrconfig-subsetquery.xml", "schema-docValuesSubsetMatch.xml");

    // sanity check our schema meets our expectations
    final IndexSchema schema = h.getCore().getLatestSchema();

    final SchemaField sf = schema.getField(f);
    assert(sf.indexed());
    final SchemaField sfCount = schema.getField(countField);
    assert(sfCount.indexed());
  }

  public void setUp() throws Exception {
    super.setUp();
    assertU(delQ("*:*"));
  }

  /** Tests the ability to do basic queries using SubsetQueryPlugin
   */
  @Test
  public void testSubsetQueryPluginSimple()  {
    assertU(adoc("id", "1", f, "a", countField, "1"));
    assertU(adoc("id", "2", f, "b", countField, "1"));
    assertU(adoc("id", "3", f, "c", countField, "1"));
    assertU(adoc("id", "4", f, "d", countField, "1"));
    assertU(adoc("id", "5", f, "a", f, "b", countField, "2"));
    assertU(adoc("id", "6", f, "a", f, "b", f, "c", countField, "3"));
    assertU(adoc("id", "7", f, "a", f, "b", f, "c", f, "d", countField, "4"));
    assertU(adoc("id", "8", f, "a", f, "b", f, "c", f, "d", f, "bar", countField, "5"));
    assertU(adoc("id", "9", countField, "0"));
    assertU(commit());

    // string: normal fq
    assertQ(req("q", "*:*", "fq", "stringdv:b", "sort", "id asc"),
        "//*[@numFound='5']",
        "//result/doc[1]/str[@name='id'][.=2]",
        "//result/doc[2]/str[@name='id'][.=5]",
        "//result/doc[3]/str[@name='id'][.=6]",
        "//result/doc[4]/str[@name='id'][.=7]",
        "//result/doc[5]/str[@name='id'][.=8]"
    );

    assertQ(req("q", "*:*", "fq", "stringdv:b", "fq", "valcount:1", "sort", "id asc"),
        "//*[@numFound='1']",
        "//result/doc[1]/str[@name='id'][.=2]"
    );

    assertQ(req("q", "*:*", "fq", "{!" + qParser + " count_field=\"valcount\" set_value=\"b\" set_field=\"stringdv\" allow_missing_val=false wildcard_token=\"foo\" }", "sort", "id asc"),
        "//*[@numFound='1']",
        "//result/doc[1]/str[@name='id'][.=2]"
    );

    assertQ(req("q", "*:*", "fq", "{!" + qParser + " count_field=\"valcount\" set_value=\"a,b\" set_field=\"stringdv\" allow_missing_val=false wildcard_token=\"foo\" }", "sort", "id asc"),
        "//*[@numFound='3']",
        "//result/doc[1]/str[@name='id'][.=1]",
        "//result/doc[2]/str[@name='id'][.=2]",
        "//result/doc[3]/str[@name='id'][.=5]"
    );

    assertQ(req("q", "*:*", "fq", "{!" + qParser + " count_field=\"valcount\" set_value=\"a,b\" set_field=\"stringdv\" allow_missing_val=true wildcard_token=\"foo\" }", "sort", "id asc"),
        "//*[@numFound='4']",
        "//result/doc[1]/str[@name='id'][.=1]",
        "//result/doc[2]/str[@name='id'][.=2]",
        "//result/doc[3]/str[@name='id'][.=5]",
        "//result/doc[4]/str[@name='id'][.=9]"
    );

    assertQ(req("q", "*:*", "fq", "{!" + qParser + " count_field=\"valcount\" set_value=\"a,b,c,d\" set_field=\"stringdv\" allow_missing_val=true wildcard_token=\"foo\" }", "sort", "id asc"),
        "//*[@numFound='8']"
    );

    assertQ(req("q", "*:*", "fq", "{!" + qParser + " count_field=\"valcount\" set_value=\"a,b,c,d\" set_field=\"stringdv\" allow_missing_val=true wildcard_token=\"bar\" }", "sort", "id asc"),
        "//*[@numFound='9']"
    );

  }

  /** Tests the missingValues capability, both +ve and -ve testing
   */
  @Test
  public void testMissingValues() throws Exception {
    assertU(adoc("id", "1", f, "a", countField, "1"));
    assertU(adoc("id", "2", countField, "0"));
    assertU(adoc("id", "3", f, "b", countField, "1"));
    assertU(adoc("id", "4", countField, "0"));
    assertU(adoc("id", "5", f, "a", f, "b", countField, "2"));
    assertU(adoc("id", "6", countField, "0"));
    assertU(adoc("id", "7", f, "a", f, "b", f, "c", countField, "3"));
    assertU(adoc("id", "8", countField, "0"));
    assertU(commit());

    // string: normal fq
    assertQ(req("q", "*:*", "sort", "id asc"),
    "//*[@numFound='8']"
    );

    // string: normal fq
    assertQ(req("q", "*:*", "fq", "stringdv:b", "sort", "id asc"),
    "//*[@numFound='3']",
    "//result/doc[1]/str[@name='id'][.=3]",
    "//result/doc[2]/str[@name='id'][.=5]",
    "//result/doc[3]/str[@name='id'][.=7]"
    );

    // Just matching docs with only b
    assertQ(req("q", "*:*", "fq", "{!" + qParser + " count_field=\"valcount\" set_value=\"b\" set_field=\"stringdv\" allow_missing_val=false wildcard_token=\"foo\" }", "sort", "id asc"),
    "//*[@numFound='1']",
    "//result/doc[1]/str[@name='id'][.=3]"
    );

    // Matching docs with only b and also the docs with no values (2, 4, 6, 8)
    assertQ(req("q", "*:*", "fq", "{!" + qParser + " count_field=\"valcount\" set_value=\"b\" set_field=\"stringdv\" allow_missing_val=true wildcard_token=\"foo\" }", "sort", "id asc"),
    "//*[@numFound='5']",
    "//result/doc[1]/str[@name='id'][.=2]",
    "//result/doc[2]/str[@name='id'][.=3]",
    "//result/doc[3]/str[@name='id'][.=4]",
    "//result/doc[4]/str[@name='id'][.=6]",
    "//result/doc[5]/str[@name='id'][.=8]"
    );

    // Matching docs with a, b or a and b
    assertQ(req("q", "*:*", "fq", "{!" + qParser + " count_field=\"valcount\" set_value=\"a,b\" set_field=\"stringdv\" allow_missing_val=false wildcard_token=\"foo\" }", "sort", "id asc"),
    "//*[@numFound='3']",
    "//result/doc[1]/str[@name='id'][.=1]",
    "//result/doc[2]/str[@name='id'][.=3]",
    "//result/doc[3]/str[@name='id'][.=5]"
    );

    // Matching docs with a, b or a and b and also the docs with no values (2, 4, 6, 8)
    assertQ(req("q", "*:*", "fq", "{!" + qParser + " count_field=\"valcount\" set_value=\"a,b\" set_field=\"stringdv\" allow_missing_val=true wildcard_token=\"foo\" }", "sort", "id asc"),
    "//*[@numFound='7']",
    "//result/doc[1]/str[@name='id'][.=1]",
    "//result/doc[2]/str[@name='id'][.=2]",
    "//result/doc[3]/str[@name='id'][.=3]",
    "//result/doc[4]/str[@name='id'][.=4]",
    "//result/doc[5]/str[@name='id'][.=5]",
    "//result/doc[6]/str[@name='id'][.=6]",
    "//result/doc[7]/str[@name='id'][.=8]"
    );
  }

  /** Tests the wildcardToken capability, both +ve and -ve testing
   * Wildcard token means you should match those documents with that term, as well as those listed
   */
  @Test
  public void testWildcardToken() throws Exception {
    assertU(adoc("id", "1", f, "a", countField, "1"));
    assertU(adoc("id", "2", f, "a", f, "foo", countField, "2"));
    assertU(adoc("id", "3", f, "b", countField, "1"));
    assertU(adoc("id", "4", f, "b", f, "foo", countField, "2"));
    assertU(adoc("id", "5", f, "a", f, "b", countField, "2"));
    assertU(adoc("id", "6", f, "a", f, "b", f, "foo", countField, "3"));
    assertU(adoc("id", "7", f, "a", f, "b", f, "c", countField, "3"));
    assertU(adoc("id", "8", f, "a", f, "b", f, "c", f, "foo", countField, "4"));
    assertU(adoc("id", "9", f, "foo", countField, "1"));
    assertU(commit());

    // string: normal fq
    assertQ(req("q", "*:*", "sort", "id asc"),
        "//*[@numFound='9']"
    );

    // string: normal fq
    assertQ(req("q", "*:*", "fq", "stringdv:b", "sort", "id asc"),
        "//*[@numFound='6']",
        "//result/doc[1]/str[@name='id'][.=3]",
        "//result/doc[2]/str[@name='id'][.=4]",
        "//result/doc[3]/str[@name='id'][.=5]",
        "//result/doc[4]/str[@name='id'][.=6]",
        "//result/doc[5]/str[@name='id'][.=7]",
        "//result/doc[6]/str[@name='id'][.=8]"
    );

    // Matching docs with only b, plus wildcard token of bar (has no matches)
    assertQ(req("q", "*:*", "fq", "{!" + qParser + " count_field=\"valcount\" set_value=\"b\" set_field=\"stringdv\" allow_missing_val=false wildcard_token=\"bar\" }", "sort", "id asc"),
        "//*[@numFound='1']",
        "//result/doc[1]/str[@name='id'][.=3]"
    );

    // Matching docs with only b, plus wildcard token of foo (matches 4 and 9)
    assertQ(req("q", "*:*", "fq", "{!" + qParser + " count_field=\"valcount\" set_value=\"b\" set_field=\"stringdv\" allow_missing_val=false wildcard_token=\"foo\" }", "sort", "id asc"),
        "//*[@numFound='3']",
        "//result/doc[1]/str[@name='id'][.=3]",
        "//result/doc[2]/str[@name='id'][.=4]",
        "//result/doc[3]/str[@name='id'][.=9]"
    );

    // Matching docs with a, b or a and b, plus wildcard token of bar (has no matches)
    assertQ(req("q", "*:*", "fq", "{!" + qParser + " count_field=\"valcount\" set_value=\"a,b\" set_field=\"stringdv\" allow_missing_val=false wildcard_token=\"bar\" }", "sort", "id asc"),
        "//*[@numFound='3']",
        "//result/doc[1]/str[@name='id'][.=1]",
        "//result/doc[2]/str[@name='id'][.=3]",
        "//result/doc[3]/str[@name='id'][.=5]"
    );

    // Matching docs with a, b or a and b, plus wildcard token of foo (matches 2, 4, 6 and 9)
    assertQ(req("q", "*:*", "fq", "{!" + qParser + " count_field=\"valcount\" set_value=\"a,b\" set_field=\"stringdv\" allow_missing_val=false wildcard_token=\"foo\" }", "sort", "id asc"),
        "//*[@numFound='7']",
        "//result/doc[1]/str[@name='id'][.=1]",
        "//result/doc[2]/str[@name='id'][.=2]",
        "//result/doc[3]/str[@name='id'][.=3]",
        "//result/doc[4]/str[@name='id'][.=4]",
        "//result/doc[5]/str[@name='id'][.=5]",
        "//result/doc[6]/str[@name='id'][.=6]",
        "//result/doc[7]/str[@name='id'][.=9]"
    );
  }

  /** Tests the wildcardToken capability and the missingValues together, both +ve and -ve testing
   */
  @Test
  public void testWildcardTokenWithMissing() throws Exception {
    assertU(adoc("id", "1", f, "a", countField, "1"));
    assertU(adoc("id", "2", f, "a", f, "foo", countField, "2"));
    assertU(adoc("id", "3", f, "b", countField, "1"));
    assertU(adoc("id", "4", f, "b", f, "foo", countField, "2"));
    assertU(adoc("id", "5", f, "a", f, "b", countField, "2"));
    assertU(adoc("id", "6", f, "a", f, "b", f, "foo", countField, "3"));
    assertU(adoc("id", "7", f, "a", f, "b", f, "c", countField, "3"));
    assertU(adoc("id", "8", f, "a", f, "b", f, "c", f, "foo", countField, "4"));
    assertU(adoc("id", "9", f, "foo", countField, "1"));
    assertU(adoc("id", "10", countField, "0"));
    assertU(commit());

    // string: normal fq
    assertQ(req("q", "*:*", "sort", "id asc"),
        "//*[@numFound='10']"
    );

    // string: normal fq
    assertQ(req("q", "*:*", "fq", "stringdv:b", "sort", "id asc"),
        "//*[@numFound='6']",
        "//result/doc[1]/str[@name='id'][.=3]",
        "//result/doc[2]/str[@name='id'][.=4]",
        "//result/doc[3]/str[@name='id'][.=5]",
        "//result/doc[4]/str[@name='id'][.=6]",
        "//result/doc[5]/str[@name='id'][.=7]",
        "//result/doc[6]/str[@name='id'][.=8]"
    );

    // Matches docs with only b, allowMissing=false, wildcard of bar
    assertQ(req("q", "*:*", "fq", "{!" + qParser + " count_field=\"valcount\" set_value=\"b\" set_field=\"stringdv\" allow_missing_val=false wildcard_token=\"bar\" }", "sort", "id asc"),
        "//*[@numFound='1']",
        "//result/doc[1]/str[@name='id'][.=3]"
    );

    // Matches docs with only b, allowMissing=false, wildcard of foo (matches 4 and 9)
    assertQ(req("q", "*:*", "fq", "{!" + qParser + " count_field=\"valcount\" set_value=\"b\" set_field=\"stringdv\" allow_missing_val=false wildcard_token=\"foo\" }", "sort", "id asc"),
        "//*[@numFound='3']",
        "//result/doc[1]/str[@name='id'][.=3]",
        "//result/doc[2]/str[@name='id'][.=4]",
        "//result/doc[3]/str[@name='id'][.=9]"
    );

    // Matches docs with only b, allowMissing=true (matches 10), wildcard of foo (matches 4 and 9)
    assertQ(req("q", "*:*", "fq", "{!" + qParser + " count_field=\"valcount\" set_value=\"b\" set_field=\"stringdv\" allow_missing_val=true wildcard_token=\"foo\" }", "sort", "id asc"),
        "//*[@numFound='4']",
        "//result/doc[1]/str[@name='id'][.=10]",
        "//result/doc[2]/str[@name='id'][.=3]",
        "//result/doc[3]/str[@name='id'][.=4]",
        "//result/doc[4]/str[@name='id'][.=9]"
    );

    // Matching docs with a, b or a and b,
    assertQ(req("q", "*:*", "fq", "{!" + qParser + " count_field=\"valcount\" set_value=\"a,b\" set_field=\"stringdv\" allow_missing_val=false wildcard_token=\"bar\" }", "sort", "id asc"),
        "//*[@numFound='3']",
        "//result/doc[1]/str[@name='id'][.=1]",
        "//result/doc[2]/str[@name='id'][.=3]",
        "//result/doc[3]/str[@name='id'][.=5]"
    );

    assertQ(req("q", "*:*", "fq", "{!" + qParser + " count_field=\"valcount\" set_value=\"a,b\" set_field=\"stringdv\" allow_missing_val=false wildcard_token=\"foo\" }", "sort", "id asc"),
        "//*[@numFound='7']",
        "//result/doc[1]/str[@name='id'][.=1]",
        "//result/doc[2]/str[@name='id'][.=2]",
        "//result/doc[3]/str[@name='id'][.=3]",
        "//result/doc[4]/str[@name='id'][.=4]",
        "//result/doc[5]/str[@name='id'][.=5]",
        "//result/doc[6]/str[@name='id'][.=6]",
        "//result/doc[7]/str[@name='id'][.=9]"
    );

    assertQ(req("q", "*:*", "fq", "{!" + qParser + " count_field=\"valcount\" set_value=\"a,b\" set_field=\"stringdv\" allow_missing_val=true wildcard_token=\"foo\" }", "sort", "id asc"),
        "//*[@numFound='8']",
        "//result/doc[1]/str[@name='id'][.=1]",
        "//result/doc[2]/str[@name='id'][.=10]",
        "//result/doc[3]/str[@name='id'][.=2]",
        "//result/doc[4]/str[@name='id'][.=3]",
        "//result/doc[5]/str[@name='id'][.=4]",
        "//result/doc[6]/str[@name='id'][.=5]",
        "//result/doc[7]/str[@name='id'][.=6]",
        "//result/doc[8]/str[@name='id'][.=9]"
    );
  }

}
