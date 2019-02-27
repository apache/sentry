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

package org.apache.sentry.provider.db.service.persistent;

import com.google.common.base.Joiner;

import org.apache.sentry.core.common.utils.SentryConstants;
import org.apache.sentry.provider.db.service.model.MSentryRole;
import org.apache.sentry.provider.db.service.model.MSentryUser;

import javax.annotation.concurrent.NotThreadSafe;
import javax.jdo.Query;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The QueryParamBuilder provides mechanism for constructing complex JDOQL queries with parameters.
 * <p>
 * There are many places where we want to construct a non-trivial parametrized query,
 * often with sub-queries. A simple query expression is just a list of conditions joined
 * by operation (either && or ||).  More complicated query may contain sub-expressions which may contain
 * further sub-expressions, for example {@code (AA && (B | (C && D)))}.
 * <p>
 * Query may contain parameters, which usually come from external sources (e.g. Thrift requests). For example,
 * to search for a record containing specific database name we can use something like
 * {@code "dbName == " + request.getDbName()}. This opens a possibility JDOQL injection with carefully
 * constructed requests. To avoid this we need to parameterize the query into something like
 * {@code "dbName == :dbMame"} and pass {@code dbName} as a query parameter. (The colon in {@code :dbName}
 * tells Datanucleus to automatically figure out the type of the {@code dbName} parameter). We collect all
 * such parameters in a map and use {@code executeWithMap()} method of the query to pass all collected
 * parameters to a query.
 * <h2>Representing the query and sub-queries</h2>
 *
 * A query is represented as
 * <ul>
 *   <li>Top-level query operation which is always && or ||</li>
 *   <li>List of strings that constitute simple subexpressions which are joined with the top level
 *   operator when the final query string is constructed</li>
 *   <li>List of sub-expressions. Each sub-expression is another QueryParamBuilder that shares
 *   the parameter map with the parent. Usually the top-level operation of the sub-expression
 *   is the inverse of the top-level operation if the parent. Since
 *   {@code (A && (B && C))} can be simplified as {@code (A && B && C)}, there is no
 *   need for parent and child to have the same top-level operation.
 *   Children are added using the {@link #newChild()} method.</li>
 * </ul>
 *
 * <h2>Constructing the string representation of a query</h2>
 *
 * Once the query is fully constructed, we can get its string reopresentation suitable for the
 * {@code setFilter()} method of {@link javax.jdo.Query} by using {@link #toString()} method
 * which does the following:
 * <ul>
 *   <li>Combines all accumulated simple subexpressions using the top-level operator</li>
 *   <li>Recursively combines children QueryParamBuilder objects using their {@link #toString()}
 *   methods</li>
 *   <li>Joins result with the top-level operation</li>
 * </ul>
 * <em>NOTE that we do not guarantee specific order of expressions within each level</em>.
 * <p>
 * The class also provides useful common methods for checking that some field is or
 * isn't <em>NULL</em> and method <em>addSet()</em> to add dynamic set of key/values<p>
 *
 * Most class methods return <em>this</em> so it is possible to chain calls together.
 * <p>
 *
 * The class is not thread-safe.
 * <p>
 * Examples:
 * <ol>
 *     <li>
 * <pre>{@code
 *   QueryParamBuilder p = newQueryParamBuilder();
 *   p.add("key1", "val1").add("key2", "val2")
 *
 *   // Returns "(this.key1 == :key1 && this.key2 == :key2)"
 *   String queryStr = p.toString();
 *
 *   // Returns map {"key1": "val1", "key2": "val2"}
 *   Map<String, Object> args = p.getArguments();
 *
 *   Query query = pm.newQuery(Foo.class);
 *   query.setFilter(queryStr);
 *   query.executeWIthMap(args);
 *   }</pre>
 * </li>
 * <li>
 * <pre>{@code
 *   QueryParamBuilder p = newQueryParamBuilder();
 *   p.add("key1", "val1").add("key2", "val2")
 *    .newChild() // Inverts logical op from && to ||
 *      .add("key3", "val3")
 *      .add("key4", "val4").
 *
 *  // Returns "(this.key1 == :val1 && this.key2 == :val2 && \
 *  //  (this.key3 == :val4 || this.key4 == val4))"
 *  String queryStr1 = p.toString()
 * }</pre>
 * </li>
 * </ol>
 *
 * @see <a href="http://www.datanucleus.org/products/datanucleus/jdo/jdoql.html">Datanucleus JDOQL</a>
 */
@NotThreadSafe
public class QueryParamBuilder {

  /**
   * Representation of the top-level query operator.
   * Query is built by joining all parts with the specified Op.
   */
  enum Op {
    AND(" && "),
    OR(" || ");

    public String toString() {
      return value;
    }

    private final String value;

    /** Constructor from string */
    Op(String val) {
      this.value = val;
    }
  }

  // Query parts that will be joined with Op
  private final List<String> queryParts = new LinkedList<>();
  // List of children - allocated lazily when children are added
  private List<QueryParamBuilder> children;
  // Query Parameters
  private final Map<String, Object> arguments;
  // paramId is used for automatically generating variable names
  private final AtomicLong paramId;
  // Join operation
  private final String operation;

  /**
   * Create new {@link QueryParamBuilder}
   * @return the default {@link QueryParamBuilder} with && top-level operation.
   */
  public static QueryParamBuilder newQueryParamBuilder() {
    return new QueryParamBuilder();
  }

  /**
   * Create new {@link QueryParamBuilder} with specific top-level operator
   * @param operation top-level operation for subexpressions
   * @return {@link QueryParamBuilder} with specified top-level operator
   */
  public static QueryParamBuilder newQueryParamBuilder(Op operation) {
    return new QueryParamBuilder(operation);
  }

  /**
   * Create a new child builder and attach to this one. Child's join operation is the
   * inverse of the parent
   * @return new child of the QueryBuilder
   */
  public QueryParamBuilder newChild() {
    // Reverse operation of this builder
    Op operation = this.operation.equals(Op.AND.toString()) ? Op.OR : Op.AND;
    return this.newChild(operation);
  }

  /**
   * Create a new child builder attached to this one
   * @param operation - join operation
   * @return new child of the QueryBuilder
   */
  private QueryParamBuilder newChild(Op operation) {
    QueryParamBuilder child = new QueryParamBuilder(this, operation);
    if (children == null) {
      children = new LinkedList<>();
    }
    children.add(child);
    return child;
  }

  /**
   * Get query arguments
   * @return query arguments as a map of arg name and arg value suitable for
   * query.executeWithMap
   */
  public Map<String, Object> getArguments() {
    return arguments;
  }

  /**
   * Get query string - reconstructs the query string from all the parts and children.
   * @return Query string which can be matched with arguments to execute a query.
   */
  @Override
  public String toString() {
    if (children == null && queryParts.isEmpty()) {
      return "";
    }
    if (children == null) {
      return "(" + Joiner.on(operation).join(queryParts) + ")";
    }
    // Concatenate our query parts with all children
    List<String> result = new LinkedList<>(queryParts);
    for (Object child: children) {
      result.add(child.toString());
    }
    return "(" + Joiner.on(operation).join(result) + ")";
  }

  /**
   * Add parameter for field fieldName with given value where value is any Object
   * @param fieldName Field name to query for
   * @param value Field value (can be any Object)
   */
  public QueryParamBuilder addObject(String fieldName, Object value) {
    return addCustomParam("this." + fieldName + " == :" + fieldName,
            fieldName, value);
  }

  /**
   * Add string parameter for field fieldName
   * @param fieldName name of the field
   * @param value String value. Value is normalized - converted to lower case and trimmed
   * @return this
   */
  public QueryParamBuilder add(String fieldName, String value) {
    return addCommon(fieldName, value, false);
  }

  /**
   * Add string parameter to field value with or without normalization
   * @param fieldName field name of the field
   * @param value String value, inserted as is if preserveCase is true, normalized otherwise
   * @param preserveCase if true, trm and lowercase the value.
   * @return this
   */
  public QueryParamBuilder add(String fieldName, String value, boolean preserveCase) {
    return addCommon(fieldName, value, preserveCase);
  }

  /**
   * Add condition that fieldName is not equal to NULL
   * @param fieldName field name to compare to NULL
   * @return this
   */
  public QueryParamBuilder addNotNull(String fieldName) {
    queryParts.add(String.format("this.%s != \"%s\"", fieldName, SentryConstants.NULL_COL));
    return this;
  }

  /**
   * Add condition that fieldName is equal to NULL
   * @param fieldName field name to compare to NULL
   * @return this
   */
  public QueryParamBuilder addNull(String fieldName) {
    queryParts.add(String.format("this.%s == \"%s\"", fieldName, SentryConstants.NULL_COL));
    return this;
  }

  /**
   * Add custom string for evaluation together with a single parameter.
   * This is used in cases where we need expression different from this.name == value
   * @param expr String expression containing ':&lt paramName&gt' somewhere
   * @param paramName parameter name
   * @param value parameter value
   * @return this
   */
  QueryParamBuilder addCustomParam(String expr, String paramName, Object value) {
    arguments.put(paramName, value);
    queryParts.add(expr);
    return this;
  }

  /**
   * Add arbitrary query string without parameters
   * @param expr String expression
   * @return this
   */
  QueryParamBuilder addString(String expr) {
    queryParts.add(expr);
    return this;
  }

  /**
   * Add common filter for set of Sentry roles. This is used to simplify creating filters for
   * privileges belonging to the specified set of roles.
   * @param query Query used for search
   * @param paramBuilder paramBuilder for parameters
   * @param roleNames set of role names
   * @return paramBuilder supplied or a new one if the supplied one is null.
   */
  public static QueryParamBuilder addRolesFilter(Query query, QueryParamBuilder paramBuilder,
                                                 Set<String> roleNames) {
    query.declareVariables(MSentryRole.class.getName() + " role");
    if (paramBuilder == null) {
      paramBuilder = new QueryParamBuilder();
    }
    if (roleNames == null || roleNames.isEmpty()) {
      return paramBuilder;
    }
    paramBuilder.newChild().addSet("role.roleName == ", roleNames, true);
    paramBuilder.addString("roles.contains(role)");
    return paramBuilder;
  }

  /**
   * Add common filter for set of Sentry users. This is used to simplify creating filters for
   * privileges belonging to the specified set of users.
   * @param query Query used for search
   * @param paramBuilder paramBuilder for parameters
   * @param userNames set of user names
   * @return paramBuilder supplied or a new one if the supplied one is null.
   */
  public static QueryParamBuilder addUsersFilter(Query query, QueryParamBuilder paramBuilder,
      Set<String> userNames) {
    query.declareVariables(MSentryUser.class.getName() + " user");
    if (paramBuilder == null) {
      paramBuilder = new QueryParamBuilder();
    }
    if (userNames == null || userNames.isEmpty()) {
      return paramBuilder;
    }
    paramBuilder.newChild().addSet("user.userName == ", userNames, false);
    paramBuilder.addString("users.contains(user)");
    return paramBuilder;
  }

  /**
   * Add common filter for set of paths. This is used to simplify creating filters for
   * a collections of paths
   * @param paramBuilder paramBuilder for parameters
   * @param paths set paths
   * @return paramBuilder supplied or a new one if the supplied one is null.
   */
  public static QueryParamBuilder addPathFilter(QueryParamBuilder paramBuilder,
                                                 Collection<String> paths) {
    if (paramBuilder == null) {
      paramBuilder = new QueryParamBuilder();
    }
    if (paths == null || paths.isEmpty()) {
      return paramBuilder;
    }
    paramBuilder.newChild().addSet("this.path == ", paths, false);
    return paramBuilder;
  }

  /**
   * Add common filter for set of actions. This is used to simplify creating filters for
   * a collections of actions
   * @param paramBuilder paramBuilder for parameters
   * @param actions set actions
   * @return paramBuilder supplied or a new one if the supplied one is null.
   */
  public static QueryParamBuilder addActionFilter(QueryParamBuilder paramBuilder,
      Collection<String> actions) {
    if (paramBuilder == null) {
      paramBuilder = new QueryParamBuilder();
    }
    if (actions == null || actions.isEmpty()) {
      return paramBuilder;
    }
    paramBuilder.newChild().addSet("this.action == ", actions, false);
    return paramBuilder;
  }

  /**
   * Add multiple conditions for set of values.
   * <p>
   * Example:
   * <pre>
   *  Set<String>names = new HashSet<>();
   *  names.add("foo");
   *  names.add("bar");
   *  names.add("bob");
   *  paramBuilder.addSet("prefix == ", names);
   *  // Expect:"(prefix == :var0 && prefix == :var1 && prefix == :var2)"
   *  paramBuilder.toString());
   *  // paramBuilder(getArguments()) contains mapping for var0, var1 and var2
   * </pre>
   * @param prefix common prefix to use for expression
   * @param values
   * @return this
   */
  QueryParamBuilder addSet(String prefix, Iterable<String> values, boolean toLowerCase) {
    if (values == null) {
      return this;
    }

    // Add expressions of the form 'prefix :var$i'
    for(String name: values) {
      // Append index to the varName
      String vName = "var" + paramId.toString();

      addCustomParam(prefix + ":" + vName, vName, (toLowerCase) ? name.trim().toLowerCase() : name.trim());
      paramId.incrementAndGet();
    }
    return this;
  }

  /**
   * Construct a default QueryParamBuilder joining arguments with &&
   */
  private QueryParamBuilder() {
    this(Op.AND);
  }

  /**
   * Construct generic QueryParamBuilder
   * @param operation join operation (AND or OR)
   */
  private QueryParamBuilder(Op operation) {
    this.arguments = new HashMap<>();
    this.operation = operation.toString();
    this.paramId = new AtomicLong(0);
  }

  /**
   * Internal constructor used for children - reuses arguments from parent
   * @param parent parent element
   * @param operation join operation
   */
  private QueryParamBuilder(QueryParamBuilder parent, Op operation) {
    this.arguments = parent.getArguments();
    this.operation = operation.toString();
    this.paramId = parent.paramId;
  }

  /**
   * common code for adding string values
   * @param fieldName field name to add
   * @param value field value
   * @param preserveCase if true, do not trim and lower
   * @return this
   * @throws IllegalArgumentException if fieldName was already added before
   */
  private QueryParamBuilder addCommon(String fieldName, String value,
                                      boolean preserveCase) {
    Object oldValue;
    if (preserveCase) {
      oldValue = arguments.put(fieldName, SentryStore.toNULLCol(SentryStore.safeTrim(value)));
    } else {
      oldValue = arguments.put(fieldName, SentryStore.toNULLCol(SentryStore.safeTrimLower(value)));
    }
    if (oldValue != null) {
      // Attempt to insert the same field twice
      throw new IllegalArgumentException("field " + fieldName + "already exists");
    }
    queryParts.add("this." + fieldName + " == :" + fieldName);
    return this;
  }
}
