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

package org.apache.sentry.provider.db.service.thrift;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.IHMSHandler;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ConfigValSecurityException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.InvalidInputException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.InvalidPartitionException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionEventType;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.PrivilegeBag;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.Type;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.metastore.api.UnknownPartitionException;
import org.apache.hadoop.hive.metastore.api.UnknownTableException;
import org.apache.sentry.policystore.api.SentryThriftPolicyService;
import org.apache.sentry.policystore.api.TAlterSentryRoleAddGroupsRequest;
import org.apache.sentry.policystore.api.TAlterSentryRoleAddGroupsResponse;
import org.apache.sentry.policystore.api.TAlterSentryRoleDeleteGroupsRequest;
import org.apache.sentry.policystore.api.TAlterSentryRoleDeleteGroupsResponse;
import org.apache.sentry.policystore.api.TCreateSentryPrivilegeRequest;
import org.apache.sentry.policystore.api.TCreateSentryPrivilegeResponse;
import org.apache.sentry.policystore.api.TCreateSentryRoleRequest;
import org.apache.sentry.policystore.api.TCreateSentryRoleResponse;
import org.apache.sentry.policystore.api.TListSentryRolesRequest;
import org.apache.sentry.policystore.api.TListSentryRolesResponse;
import org.apache.sentry.policystore.api.TSentryAlreadyExistsException;
import org.apache.sentry.policystore.api.TSentryNoSuchObjectException;
import org.apache.thrift.TException;

import com.facebook.fb303.fb_status;

public class HiveMetaStoreSentryPolicyStoreHandler
  implements SentryThriftPolicyService.Iface, IHMSHandler {
  private final String name;
  private HiveConf conf;
  
  private final SentryPolicyStoreHandler sentryPolicyStoreHander;
  private final IHMSHandler hiveMetaStoreHandler;
  
  public HiveMetaStoreSentryPolicyStoreHandler(String name, HiveConf conf)
      throws MetaException {
    super();
    this.name = name;
    this.conf = conf;
    sentryPolicyStoreHander = new SentryPolicyStoreHandler(name, conf);
    hiveMetaStoreHandler = new HiveMetaStore.HMSHandler(name, conf);
  }

  @Override
  public TCreateSentryRoleResponse create_sentry_role(
      TCreateSentryRoleRequest request) throws TSentryAlreadyExistsException,
      TException {
    return sentryPolicyStoreHander.create_sentry_role(request);
  }
  @Override
  public TCreateSentryPrivilegeResponse create_sentry_privilege(
      TCreateSentryPrivilegeRequest request)
      throws TSentryAlreadyExistsException, TException {
    return sentryPolicyStoreHander.create_sentry_privilege(request);
  }
  @Override
  public TAlterSentryRoleAddGroupsResponse alter_sentry_role_add_groups(
      TAlterSentryRoleAddGroupsRequest request)
      throws TSentryNoSuchObjectException, TException {
    return sentryPolicyStoreHander.alter_sentry_role_add_groups(request);
  }
  @Override
  public TAlterSentryRoleDeleteGroupsResponse alter_sentry_role_delete_groups(
      TAlterSentryRoleDeleteGroupsRequest request)
      throws TSentryNoSuchObjectException, TException {
    return sentryPolicyStoreHander.alter_sentry_role_delete_groups(request);
  }
  @Override
  public TListSentryRolesResponse list_sentry_roles(
      TListSentryRolesRequest request) throws TSentryNoSuchObjectException,
      TException {
    return sentryPolicyStoreHander.list_sentry_roles(request);
  }

  // below is hive methods

  @Override
  public Index add_index(Index arg0, Table arg1) throws InvalidObjectException,
      org.apache.hadoop.hive.metastore.api.AlreadyExistsException,
      MetaException, TException {
    return hiveMetaStoreHandler.add_index(arg0, arg1);
  }

  @Override
  public Partition add_partition(Partition arg0) throws InvalidObjectException,
      org.apache.hadoop.hive.metastore.api.AlreadyExistsException,
      MetaException, TException {
    return hiveMetaStoreHandler.add_partition(arg0);
  }

  @Override
  public Partition add_partition_with_environment_context(Partition arg0,
      EnvironmentContext arg1) throws InvalidObjectException,
      org.apache.hadoop.hive.metastore.api.AlreadyExistsException,
      MetaException, TException {
    return hiveMetaStoreHandler.add_partition_with_environment_context(arg0, arg1);
  }

  @Override
  public int add_partitions(List<Partition> arg0)
      throws InvalidObjectException,
      org.apache.hadoop.hive.metastore.api.AlreadyExistsException,
      MetaException, TException {
    return hiveMetaStoreHandler.add_partitions(arg0);
  }

  @Override
  public void alter_database(String arg0, Database arg1) throws MetaException,
      org.apache.hadoop.hive.metastore.api.NoSuchObjectException, TException {
    hiveMetaStoreHandler.alter_database(arg0, arg1);
  }

  @Override
  public void alter_index(String arg0, String arg1, String arg2, Index arg3)
      throws InvalidOperationException, MetaException, TException {
    hiveMetaStoreHandler.alter_index(arg0, arg1, arg2, arg3);
  }

  @Override
  public void alter_partition(String arg0, String arg1, Partition arg2)
      throws InvalidOperationException, MetaException, TException {
    hiveMetaStoreHandler.alter_partition(arg0, arg1, arg2);
  }

  @Override
  public void alter_partition_with_environment_context(String arg0,
      String arg1, Partition arg2, EnvironmentContext arg3)
      throws InvalidOperationException, MetaException, TException {
    hiveMetaStoreHandler.alter_partition_with_environment_context(arg0, arg1, arg2, arg3);
  }

  @Override
  public void alter_partitions(String arg0, String arg1, List<Partition> arg2)
      throws InvalidOperationException, MetaException, TException {
    hiveMetaStoreHandler.alter_partitions(arg0, arg1, arg2);
  }

  @Override
  public void alter_table(String arg0, String arg1, Table arg2)
      throws InvalidOperationException, MetaException, TException {
    hiveMetaStoreHandler.alter_table(arg0, arg1, arg2);
  }

  @Override
  public void alter_table_with_environment_context(String arg0, String arg1,
      Table arg2, EnvironmentContext arg3) throws InvalidOperationException,
      MetaException, TException {
    hiveMetaStoreHandler.alter_table_with_environment_context(arg0, arg1, arg2, arg3);
  }

  @Override
  public Partition append_partition(String arg0, String arg1, List<String> arg2)
      throws InvalidObjectException,
      org.apache.hadoop.hive.metastore.api.AlreadyExistsException,
      MetaException, TException {
    return hiveMetaStoreHandler.append_partition(arg0, arg1, arg2);
  }

  @Override
  public Partition append_partition_by_name(String arg0, String arg1,
      String arg2) throws InvalidObjectException,
      org.apache.hadoop.hive.metastore.api.AlreadyExistsException,
      MetaException, TException {
    return hiveMetaStoreHandler.append_partition_by_name(arg0, arg1, arg2);
  }

  @Override
  public void cancel_delegation_token(String arg0) throws MetaException,
      TException {
    hiveMetaStoreHandler.cancel_delegation_token(arg0);
  }

  @Override
  public void create_database(Database arg0)
      throws org.apache.hadoop.hive.metastore.api.AlreadyExistsException,
      InvalidObjectException, MetaException, TException {
    hiveMetaStoreHandler.create_database(arg0);
  }

  @Override
  public boolean create_role(Role arg0) throws MetaException, TException {
    return hiveMetaStoreHandler.create_role(arg0);
  }

  @Override
  public void create_table(Table arg0)
      throws org.apache.hadoop.hive.metastore.api.AlreadyExistsException,
      InvalidObjectException, MetaException,
      org.apache.hadoop.hive.metastore.api.NoSuchObjectException, TException {
    hiveMetaStoreHandler.create_table(arg0);
  }

  @Override
  public void create_table_with_environment_context(Table arg0,
      EnvironmentContext arg1)
      throws org.apache.hadoop.hive.metastore.api.AlreadyExistsException,
      InvalidObjectException, MetaException,
      org.apache.hadoop.hive.metastore.api.NoSuchObjectException, TException {
    hiveMetaStoreHandler.create_table_with_environment_context(arg0, arg1);
  }

  @Override
  public boolean create_type(Type arg0)
      throws org.apache.hadoop.hive.metastore.api.AlreadyExistsException,
      InvalidObjectException, MetaException, TException {
    return hiveMetaStoreHandler.create_type(arg0);
  }

  @Override
  public boolean delete_partition_column_statistics(String arg0, String arg1,
      String arg2, String arg3)
      throws org.apache.hadoop.hive.metastore.api.NoSuchObjectException,
      MetaException, InvalidObjectException, InvalidInputException, TException {
    return hiveMetaStoreHandler.delete_partition_column_statistics(arg0, arg1, arg2, arg3);
  }

  @Override
  public boolean delete_table_column_statistics(String arg0, String arg1,
      String arg2)
      throws org.apache.hadoop.hive.metastore.api.NoSuchObjectException,
      MetaException, InvalidObjectException, InvalidInputException, TException {
    return hiveMetaStoreHandler.delete_table_column_statistics(arg0, arg1, arg2);
  }

  @Override
  public void drop_database(String arg0, boolean arg1, boolean arg2)
      throws org.apache.hadoop.hive.metastore.api.NoSuchObjectException,
      InvalidOperationException, MetaException, TException {
    hiveMetaStoreHandler.drop_database(arg0, arg1, arg2);
  }

  @Override
  public boolean drop_index_by_name(String arg0, String arg1, String arg2,
      boolean arg3)
      throws org.apache.hadoop.hive.metastore.api.NoSuchObjectException,
      MetaException, TException {
    return hiveMetaStoreHandler.drop_index_by_name(arg0, arg1, arg2, arg3);
  }

  @Override
  public boolean drop_partition(String arg0, String arg1, List<String> arg2,
      boolean arg3)
      throws org.apache.hadoop.hive.metastore.api.NoSuchObjectException,
      MetaException, TException {
    return hiveMetaStoreHandler.drop_partition(arg0, arg1, arg2, arg3);
  }

  @Override
  public boolean drop_partition_by_name(String arg0, String arg1, String arg2,
      boolean arg3)
      throws org.apache.hadoop.hive.metastore.api.NoSuchObjectException,
      MetaException, TException {
    return hiveMetaStoreHandler.drop_index_by_name(arg0, arg1, arg2, arg3);
  }

  @Override
  public boolean drop_role(String arg0) throws MetaException, TException {
    return hiveMetaStoreHandler.drop_role(arg0);
  }

  @Override
  public void drop_table(String arg0, String arg1, boolean arg2)
      throws org.apache.hadoop.hive.metastore.api.NoSuchObjectException,
      MetaException, TException {
    hiveMetaStoreHandler.drop_table(arg0, arg1, arg2);
  }

  @Override
  public boolean drop_type(String arg0) throws MetaException,
      org.apache.hadoop.hive.metastore.api.NoSuchObjectException, TException {
    return hiveMetaStoreHandler.drop_type(arg0);
  }

  @Override
  public List<String> get_all_databases() throws MetaException, TException {
    return hiveMetaStoreHandler.get_all_databases();
  }

  @Override
  public List<String> get_all_tables(String arg0) throws MetaException,
      TException {
    return hiveMetaStoreHandler.get_all_tables(arg0);
  }

  @Override
  public String get_config_value(String arg0, String arg1)
      throws ConfigValSecurityException, TException {
    return hiveMetaStoreHandler.get_config_value(arg0, arg1);
  }

  @Override
  public Database get_database(String arg0)
      throws org.apache.hadoop.hive.metastore.api.NoSuchObjectException,
      MetaException, TException {
    return hiveMetaStoreHandler.get_database(arg0);
  }

  @Override
  public List<String> get_databases(String arg0) throws MetaException,
      TException {
    return hiveMetaStoreHandler.get_databases(arg0);
  }

  @Override
  public String get_delegation_token(String arg0, String arg1)
      throws MetaException, TException {
    return hiveMetaStoreHandler.get_delegation_token(arg0, arg1);
  }

  @Override
  public List<FieldSchema> get_fields(String arg0, String arg1)
      throws MetaException, UnknownTableException, UnknownDBException,
      TException {
    return hiveMetaStoreHandler.get_fields(arg0, arg1);
  }

  @Override
  public Index get_index_by_name(String arg0, String arg1, String arg2)
      throws MetaException,
      org.apache.hadoop.hive.metastore.api.NoSuchObjectException, TException {
    return hiveMetaStoreHandler.get_index_by_name(arg0, arg1, arg2);
  }

  @Override
  public List<String> get_index_names(String arg0, String arg1, short arg2)
      throws MetaException, TException {
    return hiveMetaStoreHandler.get_index_names(arg0, arg1, arg2);
  }

  @Override
  public List<Index> get_indexes(String arg0, String arg1, short arg2)
      throws org.apache.hadoop.hive.metastore.api.NoSuchObjectException,
      MetaException, TException {
    return hiveMetaStoreHandler.get_indexes(arg0, arg1, arg2);
  }

  @Override
  public Partition get_partition(String arg0, String arg1, List<String> arg2)
      throws MetaException,
      org.apache.hadoop.hive.metastore.api.NoSuchObjectException, TException {
    return hiveMetaStoreHandler.get_partition(arg0, arg1, arg2);
  }

  @Override
  public Partition get_partition_by_name(String arg0, String arg1, String arg2)
      throws MetaException,
      org.apache.hadoop.hive.metastore.api.NoSuchObjectException, TException {
    return hiveMetaStoreHandler.get_partition_by_name(arg0, arg1, arg2);
  }

  @Override
  public ColumnStatistics get_partition_column_statistics(String arg0,
      String arg1, String arg2, String arg3)
      throws org.apache.hadoop.hive.metastore.api.NoSuchObjectException,
      MetaException, InvalidInputException, InvalidObjectException, TException {
    return hiveMetaStoreHandler.get_partition_column_statistics(arg0, arg1, arg2, arg3);
  }

  @Override
  public List<String> get_partition_names(String arg0, String arg1, short arg2)
      throws MetaException, TException {
    return hiveMetaStoreHandler.get_partition_names(arg0, arg1, arg2);
  }

  @Override
  public List<String> get_partition_names_ps(String arg0, String arg1,
      List<String> arg2, short arg3) throws MetaException,
      org.apache.hadoop.hive.metastore.api.NoSuchObjectException, TException {
    return hiveMetaStoreHandler.get_partition_names_ps(arg0, arg1, arg2, arg3);
  }

  @Override
  public Partition get_partition_with_auth(String arg0, String arg1,
      List<String> arg2, String arg3, List<String> arg4) throws MetaException,
      org.apache.hadoop.hive.metastore.api.NoSuchObjectException, TException {
    return hiveMetaStoreHandler.get_partition_with_auth(arg0, arg1, arg2, arg3, arg4);
  }

  @Override
  public List<Partition> get_partitions(String arg0, String arg1, short arg2)
      throws org.apache.hadoop.hive.metastore.api.NoSuchObjectException,
      MetaException, TException {
    return hiveMetaStoreHandler.get_partitions(arg0, arg1, arg2);
  }

  @Override
  public List<Partition> get_partitions_by_filter(String arg0, String arg1,
      String arg2, short arg3) throws MetaException,
      org.apache.hadoop.hive.metastore.api.NoSuchObjectException, TException {
    return hiveMetaStoreHandler.get_partitions_by_filter(arg0, arg1, arg2, arg3);
  }

  @Override
  public List<Partition> get_partitions_by_names(String arg0, String arg1,
      List<String> arg2) throws MetaException,
      org.apache.hadoop.hive.metastore.api.NoSuchObjectException, TException {
    return hiveMetaStoreHandler.get_partitions_by_names(arg0, arg1, arg2);
  }

  @Override
  public List<Partition> get_partitions_ps(String arg0, String arg1,
      List<String> arg2, short arg3) throws MetaException,
      org.apache.hadoop.hive.metastore.api.NoSuchObjectException, TException {
    return hiveMetaStoreHandler.get_partitions_ps(arg0, arg1, arg2, arg3);
  }

  @Override
  public List<Partition> get_partitions_ps_with_auth(String arg0, String arg1,
      List<String> arg2, short arg3, String arg4, List<String> arg5)
      throws org.apache.hadoop.hive.metastore.api.NoSuchObjectException,
      MetaException, TException {
    return hiveMetaStoreHandler.get_partitions_ps_with_auth(arg0, arg1, arg2, arg3, arg4, arg5);
  }

  @Override
  public List<Partition> get_partitions_with_auth(String arg0, String arg1,
      short arg2, String arg3, List<String> arg4)
      throws org.apache.hadoop.hive.metastore.api.NoSuchObjectException,
      MetaException, TException {
    return hiveMetaStoreHandler.get_partitions_with_auth(arg0, arg1, arg2, arg3, arg4);
  }

  @Override
  public PrincipalPrivilegeSet get_privilege_set(HiveObjectRef arg0,
      String arg1, List<String> arg2) throws MetaException, TException {
    return hiveMetaStoreHandler.get_privilege_set(arg0, arg1, arg2);
  }

  @Override
  public List<String> get_role_names() throws MetaException, TException {
    return hiveMetaStoreHandler.get_role_names();
  }

  @Override
  public List<FieldSchema> get_schema(String arg0, String arg1)
      throws MetaException, UnknownTableException, UnknownDBException,
      TException {
    return hiveMetaStoreHandler.get_schema(arg0, arg1);
  }

  @Override
  public Table get_table(String arg0, String arg1) throws MetaException,
      org.apache.hadoop.hive.metastore.api.NoSuchObjectException, TException {
    return hiveMetaStoreHandler.get_table(arg0, arg1);
  }

  @Override
  public ColumnStatistics get_table_column_statistics(String arg0, String arg1,
      String arg2)
      throws org.apache.hadoop.hive.metastore.api.NoSuchObjectException,
      MetaException, InvalidInputException, InvalidObjectException, TException {
    return hiveMetaStoreHandler.get_table_column_statistics(arg0, arg1, arg2);
  }

  @Override
  public List<String> get_table_names_by_filter(String arg0, String arg1,
      short arg2) throws MetaException, InvalidOperationException,
      UnknownDBException, TException {
    return hiveMetaStoreHandler.get_table_names_by_filter(arg0, arg1, arg2);
  }

  @Override
  public List<Table> get_table_objects_by_name(String arg0, List<String> arg1)
      throws MetaException, InvalidOperationException, UnknownDBException,
      TException {
    return hiveMetaStoreHandler.get_table_objects_by_name(arg0, arg1);
  }

  @Override
  public List<String> get_tables(String arg0, String arg1)
      throws MetaException, TException {
    return hiveMetaStoreHandler.get_tables(arg0, arg1);
  }

  @Override
  public Type get_type(String arg0) throws MetaException,
      org.apache.hadoop.hive.metastore.api.NoSuchObjectException, TException {
    return hiveMetaStoreHandler.get_type(arg0);
  }

  @Override
  public Map<String, Type> get_type_all(String arg0) throws MetaException,
      TException {
    return hiveMetaStoreHandler.get_type_all(arg0);
  }

  @Override
  public boolean grant_privileges(PrivilegeBag arg0) throws MetaException,
      TException {
    return hiveMetaStoreHandler.grant_privileges(arg0);
  }

  @Override
  public boolean grant_role(String arg0, String arg1, PrincipalType arg2,
      String arg3, PrincipalType arg4, boolean arg5) throws MetaException,
      TException {
    return hiveMetaStoreHandler.grant_role(arg0, arg1, arg2, arg3, arg4, arg5);
  }

  @Override
  public boolean isPartitionMarkedForEvent(String arg0, String arg1,
      Map<String, String> arg2, PartitionEventType arg3) throws MetaException,
      org.apache.hadoop.hive.metastore.api.NoSuchObjectException,
      UnknownDBException, UnknownTableException, UnknownPartitionException,
      InvalidPartitionException, TException {
    return hiveMetaStoreHandler.isPartitionMarkedForEvent(arg0, arg1, arg2, arg3);
  }

  @Override
  public List<HiveObjectPrivilege> list_privileges(String arg0,
      PrincipalType arg1, HiveObjectRef arg2) throws MetaException, TException {
    return hiveMetaStoreHandler.list_privileges(arg0, arg1, arg2);
  }

  @Override
  public List<Role> list_roles(String arg0, PrincipalType arg1)
      throws MetaException, TException {
    return hiveMetaStoreHandler.list_roles(arg0, arg1);
  }

  @Override
  public void markPartitionForEvent(String arg0, String arg1,
      Map<String, String> arg2, PartitionEventType arg3) throws MetaException,
      org.apache.hadoop.hive.metastore.api.NoSuchObjectException,
      UnknownDBException, UnknownTableException, UnknownPartitionException,
      InvalidPartitionException, TException {
    hiveMetaStoreHandler.markPartitionForEvent(arg0, arg1, arg2, arg3);
  }

  @Override
  public Map<String, String> partition_name_to_spec(String arg0)
      throws MetaException, TException {
    return hiveMetaStoreHandler.partition_name_to_spec(arg0);
  }

  @Override
  public List<String> partition_name_to_vals(String arg0) throws MetaException,
      TException {
    return hiveMetaStoreHandler.partition_name_to_vals(arg0);
  }

  @Override
  public void rename_partition(String arg0, String arg1, List<String> arg2,
      Partition arg3) throws InvalidOperationException, MetaException,
      TException {
    hiveMetaStoreHandler.rename_partition(arg0, arg1, arg2, arg3);
  }

  @Override
  public long renew_delegation_token(String arg0) throws MetaException,
      TException {
    return hiveMetaStoreHandler.renew_delegation_token(arg0);
  }

  @Override
  public boolean revoke_privileges(PrivilegeBag arg0) throws MetaException,
      TException {
    return hiveMetaStoreHandler.revoke_privileges(arg0);
  }

  @Override
  public boolean revoke_role(String arg0, String arg1, PrincipalType arg2)
      throws MetaException, TException {
    return hiveMetaStoreHandler.revoke_role(arg0, arg1, arg2);
  }

  @Override
  public List<String> set_ugi(String arg0, List<String> arg1)
      throws MetaException, TException {
    return hiveMetaStoreHandler.set_ugi(arg0, arg1);
  }

  @Override
  public boolean update_partition_column_statistics(ColumnStatistics arg0)
      throws org.apache.hadoop.hive.metastore.api.NoSuchObjectException,
      InvalidObjectException, MetaException, InvalidInputException, TException {
    return hiveMetaStoreHandler.update_partition_column_statistics(arg0);
  }

  @Override
  public boolean update_table_column_statistics(ColumnStatistics arg0)
      throws org.apache.hadoop.hive.metastore.api.NoSuchObjectException,
      InvalidObjectException, MetaException, InvalidInputException, TException {
    return hiveMetaStoreHandler.update_table_column_statistics(arg0);
  }

  @Override
  public long aliveSince() throws TException {
    return hiveMetaStoreHandler.aliveSince();
  }

  @Override
  public long getCounter(String arg0) throws TException {
    return hiveMetaStoreHandler.getCounter(arg0);
  }

  @Override
  public Map<String, Long> getCounters() throws TException {
    return hiveMetaStoreHandler.getCounters();
  }

  @Override
  public String getCpuProfile(int arg0) throws TException {
    return hiveMetaStoreHandler.getCpuProfile(arg0);
  }

  @Override
  public String getName() throws TException {
    return hiveMetaStoreHandler.getName();
  }

  @Override
  public String getOption(String arg0) throws TException {
    return hiveMetaStoreHandler.getOption(arg0);
  }

  @Override
  public Map<String, String> getOptions() throws TException {
    return hiveMetaStoreHandler.getOptions();
  }

  @Override
  public fb_status getStatus() throws TException {
    return hiveMetaStoreHandler.getStatus();
  }

  @Override
  public String getStatusDetails() throws TException {
    return hiveMetaStoreHandler.getStatusDetails();
  }

  @Override
  public String getVersion() throws TException {
    return hiveMetaStoreHandler.getVersion();
  }

  @Override
  public void reinitialize() throws TException {
    hiveMetaStoreHandler.reinitialize();
    
  }

  @Override
  public void setOption(String arg0, String arg1) throws TException {
    hiveMetaStoreHandler.setOption(arg0, arg1);
    
  }

  @Override
  public void shutdown() throws TException {
    hiveMetaStoreHandler.shutdown();
  }

  @Override
  public void setConf(Configuration arg0) {
    hiveMetaStoreHandler.setConf(arg0);
  }
}
