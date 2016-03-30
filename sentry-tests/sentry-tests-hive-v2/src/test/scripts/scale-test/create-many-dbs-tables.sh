#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# This script means to create many testing objects (database, tables,
# partitions and a wide table with many partitions). The way to run it:
# !/usr/bin/env bash
# export HS2="HOSTNAME"
# export REALM="REALM.NAME"
# bash /root/tests/create-many-dbs-tables.sh &
# bash /root/tests/create-many-dbs-tables.sh &

if [[ ${HS2} == "" ]]; then
  echo "error: need to export HS2=hostname"
  exit 1
fi

if [[ ${REALM} == "" ]]; then
  echo "error: need to export REALM"
  exit 1
fi

# Define default test scale
NUM_OF_DATABASES=60
NUM_OF_TABLES_PER_DATABASE=20
NUM_OF_ROLES_FOR_DATABASES=60 # <= NUM_OF_DATABASES
NUM_OF_ROLES_FOR_TABLES_PER_DATABASE=5 # <= NUM_OF_TABLES_PER_DATABASE
NUM_OF_GROUPS=60 # >= NUM_OF_DATABASES

# Number of partitions varies between max and min
MAX_NUM_OF_PARTITIONS_PER_TABLE=10
MIN_NUM_OF_PARTITIONS_PER_TABLE=2

BASE_EXTERNAL_DIR="/data"
LOCAL_OUTPUT_DIR="/tmp"
BL="beeline -n hive -p hive --silent=true -u 'jdbc:hive2://${HS2}:10000/default;principal=hive/_HOST@${REALM}'"

# Number of external partitions wide tables have
declare -a NUM_OF_WIDE_TABLE_PARTITIONS=(10 100 1000)
wLen=${#NUM_OF_WIDE_TABLE_PARTITIONS[@]}

process_id=$$

while getopts "d:t:g:b:l" OPTION
do	case "${OPTION}" in
  b)      BASE_EXTERNAL_DIR="$OPTARG";;
  d)	  NUM_OF_DATABASES="$OPTARG";;
  l)      LOCAL_OUTPUT_DIR="$OPTARG";;
  t)      NUM_OF_TABLES_PER_DATABASE="$OPTARG";;
  g)	  NUM_OF_GROUPS="$OPTARG";;
  [?])	  print >&2 "Usage: $0 [-b BASE_EXTERNAL_DIR] [-d NUM_OF_DATABASES] [-l LOCAL_OUTPUT_DIR] [-t NUM_OF_TABLES_PER_DATABASE] [-g NUM_OF_GROUPS]"
	  exit 1;;
	esac
done

NUM_OF_PERMISSIONS=$(( NUM_OF_ROLES_FOR_DATABASES + NUM_OF_ROLES_FOR_TABLES_PER_DATABASE * NUM_OF_DATABASES))
AVG_NUM_OF_PARTITIONS_PER_TABLE=$((( MAX_NUM_OF_PARTITIONS_PER_TABLE + MIN_NUM_OF_PARTITIONS_PER_TABLE) / 2 ))

echo "[${process_id}]	Scale numbers:"
echo "[${process_id}]	number of databases: ${NUM_OF_DATABASES}"
echo "[${process_id}]	number of tables: $((NUM_OF_TABLES_PER_DATABASE * NUM_OF_DATABASES))"
echo "[${process_id}]	number of wide tables: ${wLen}"
echo "[${process_id}]	number of partitions per table: ${AVG_NUM_OF_PARTITIONS_PER_TABLE}"
echo "[${process_id}]	number of min partitions per wide table: ${NUM_OF_WIDE_TABLE_PARTITIONS[0]}"
echo "[${process_id}]	number of max partitions per wide table: ${NUM_OF_WIDE_TABLE_PARTITIONS[${wLen}-1]}"
echo "[${process_id}]	number of permissions: ${NUM_OF_PERMISSIONS}"
echo "[${process_id}]	number of groups: ${NUM_OF_GROUPS}"

# Random string as prefix for test databases and tables
prefix_string=$(cat /dev/urandom | tr -dc 'a-z' | fold -w 4 | head -n 1)
prefix_string=${prefix_string}$(date +%s | cut -c1-4)

DB_NAME=${prefix_string}_db

function validate_ret () {
  ret=$1
  if [[ ${ret} != "" && ${ret} -ne 0 ]]; then
    echo "ERROR!! when running query in bulk mode"
    exit $ret
  fi
}

function get_group () {
  count=$1
  group_name=group_$((count % NUM_OF_GROUPS))
  echo "$group_name"
}

# Create groups
function create_groups () {
  for g in $(seq ${NUM_OF_GROUPS}); do
    group_name=$(get_group $g)
    getent passwd ${group_name} | grep "${group_name}" 1>&2>/dev/null
    if [[ $? -ne 0 ]]; then
      sudo groupadd ${group_name}
      sudo useradd -g ${group_name} ${group_name}
    fi
  done
}

# Convenience function to create one table with many external partitons
function create_wide_table () {
  db_name=$1
  tbl_name=$2
  num_of_pars=$3
  file_name=$4
  dir_file_name=$5
  echo "-- [${process_id}]	Create ${tbl_name} in ${db_name} with ${num_of_pars} external partitions; " >> ${file_name}
  echo "CREATE DATABASE IF NOT EXISTS ${db_name}; " >> ${file_name}
  echo "USE ${db_name};" >> ${file_name}
  table_dir=${BASE_EXTERNAL_DIR}/${db_name}/${tbl_name}
  echo "sudo -u hdfs hdfs dfs -rm -R -skipTrash ${table_dir} 2>/dev/null" >> ${dir_file_name}
  echo "DROP TABLE IF EXISTS ${tbl_name}; " >> ${file_name}
  echo "CREATE TABLE ${tbl_name} (s STRING, i INT) PARTITIONED BY (par INT);" >> ${file_name}
  echo "-- create ${num_of_pars} partitions on table ${tbl_name}" >> ${file_name}
  for p in $(seq ${num_of_pars}); do
    dir=${table_dir}/$p
    echo "sudo -u hdfs hdfs dfs -mkdir -p ${dir}" >> ${dir_file_name}
    echo "ALTER TABLE ${tbl_name} ADD PARTITION (par=$p) LOCATION '${dir}';" >> ${file_name}
  done
}

# Convenience function to create wide tables with many external partitions
function create_external_par_dirs_bulk_file () {
  file_name=$1
  dir_file_name=$2
  echo "-- [${process_id}]	Start bulk process to create wide tables" > ${file_name}
  echo "# [${process_id}]	Start to create external dirs for partitions" > ${dir_file_name}
  db_id=$(awk -v n="${NUM_OF_DATABASES}" 'BEGIN{srand();print int(rand()*n+1)}')
  db_name=${DB_NAME}_${db_id}
  for p in "${!NUM_OF_WIDE_TABLE_PARTITIONS[@]}"; do
    tbl_name=${db_name}_wide_tbl_$p
	  create_wide_table ${db_name} ${tbl_name} ${NUM_OF_WIDE_TABLE_PARTITIONS[p]} ${file_name} ${dir_file_name}
  done
  chmod a+x ${file_name}
  chmod a+x ${dir_file_name}
}

# Create internal databases and their tables in one bulk file
function create_dbs_tbls_bulk_file () {
  file_name=$1
  echo "-- [${process_id}]	start bulk load " > ${file_name}
  for d in $(seq ${NUM_OF_DATABASES}); do
    db_name=${DB_NAME}_${d}
    echo "drop database if exists ${db_name}; " >> ${file_name}
    echo "create database ${db_name}; " >> ${file_name}
    echo "use ${db_name};" >> ${file_name}
    NUM_OF_COLS=$(awk -v mn="${MIN_NUM_OF_PARTITIONS_PER_TABLE}" -v mx="${MAX_NUM_OF_PARTITIONS_PER_TABLE}" 'BEGIN{srand();print int(rand()*(mx-mn)+1)}')
    NUM_OF_PARS=$(awk -v mn="${MIN_NUM_OF_PARTITIONS_PER_TABLE}" -v mx="${MAX_NUM_OF_PARTITIONS_PER_TABLE}" 'BEGIN{srand();print int(rand()*(mx-mn)+1)}')

    for t in $(seq ${NUM_OF_TABLES_PER_DATABASE}); do
      tbl_name=${db_name}_tbl_${t}
      # create table
      echo "create table ${tbl_name} (col_start INT, " >> ${file_name}
      for c in $(seq ${NUM_OF_COLS}); do
        echo "col_${c} STRING, " >> ${file_name}
      done
      echo "col_end INT) partitioned by (par_start STRING, " >> ${file_name}
      # create many partitions
      for p in $(seq ${NUM_OF_PARS}); do
        echo "par_${p} INT, " >> ${file_name}
      done
      echo "par_end STRING); " >> ${file_name}
    done
  done
  chmod a+x ${file_name}
}

# Create database roles
function create_dbs_roles () {
  db_file_name=$1
  total_db_permissions=0
  echo "-- [${process_id}] Start to create database roles" > ${db_file_name}
  for d in $(seq ${NUM_OF_ROLES_FOR_DATABASES}); do
    db_name=${DB_NAME}_${d}
    role_name=${db_name}_db_role_${d}
    group_name=$(get_group $d)
    echo "create role ${role_name}; " >> ${db_file_name}
    echo "grant all on database ${db_name} to role ${role_name}; " >> ${db_file_name}
    echo "grant ${role_name} to group ${group_name};" >> ${db_file_name}
  done
  chmod a+x ${db_file_name}
}

# Create table roles
function create_tbls_roles () {
  tbl_file_name=$1
  echo "-- [${process_id}] Start to create table roles;" > ${tbl_file_name}
  # create table roles
  for d in $(seq ${NUM_OF_DATABASES}); do
    db_name=${DB_NAME}_${d}
    echo "USE ${db_name};" >> ${tbl_file_name}
    for t in $(seq ${NUM_OF_ROLES_FOR_TABLES_PER_DATABASE}); do
      tbl_name=${db_name}_tbl_${t}
      role_name=${tbl_name}_role_${t}
      echo "CREATE ROLE ${role_name};" >> ${tbl_file_name}
      rand_number=$(awk 'BEGIN{srand();print int(rand()*3)}')
      case "$((rand_number % 3))" in
          0) echo "grant all on table ${tbl_name} to role ${role_name}; " >> ${tbl_file_name}
             ;;
          1) echo "grant insert on table ${tbl_name} to role ${role_name}; "  >> ${tbl_file_name}
             ;;
          *) echo "grant select on table ${tbl_name} to role ${role_name}; " >> ${tbl_file_name}
             ;;
      esac
      group_name=$(get_group $d)
      echo "grant role ${role_name} to group ${group_name}; "  >> ${tbl_file_name}
    done
  done
  chmod a+x ${tbl_file_name}
}

###########################
# Start from here!
###########################
create_groups
echo "# [${process_id}]	Created ${NUM_OF_GROUPS} groups"

# Use Hive to create the partitions because it supports bulk adding of partitions.
# Hive doesn't allow fully qualified table names in ALTER statements, so start with a
# USE <db>.
create_tables_file_name=${LOCAL_OUTPUT_DIR}/hive_${prefix_string}_bulk_tables.q
create_dbs_tbls_bulk_file ${create_tables_file_name}
echo "# [${process_id}]	Created ${create_tables_file_name} to create databases and tables in bulk mode"

create_wide_tables_file_name=${LOCAL_OUTPUT_DIR}/hive_${prefix_string}_bulk_wide_tables.q
create_wide_tables_dir_file_name=${LOCAL_OUTPUT_DIR}/hive_${prefix_string}_bulk_wide_tables_dirs.sh
create_external_par_dirs_bulk_file ${create_wide_tables_file_name} ${create_wide_tables_dir_file_name}
echo "# [${process_id}]	Created ${create_wide_tables_file_name} to create wide tables with external partitions in bulk mode"
echo "# [${process_id}]	Created ${create_wide_tables_dir_file_name} to create external dirs for external partitions in bulk mode"

create_db_role_file_name=${LOCAL_OUTPUT_DIR}/hive_${prefix_string}_bulk_db_roles.q
create_dbs_roles ${create_db_role_file_name}
echo "# [${process_id}]	Created ${create_db_role_file_name} to create database roles"

create_tbl_role_file_name=${LOCAL_OUTPUT_DIR}/hive_${prefix_string}_bulk_tbl_roles.q
create_tbls_roles ${create_tbl_role_file_name}
echo "# [${process_id}]	Created ${create_tbl_role_file_name} to create table roles"

sudo -u hive hive -S -f ${create_tables_file_name}
validate_ret $?
echo "# [${process_id}]	Succeessfully ran bulk file ${create_tables_file_name} to create databases and tables"

. ${create_wide_tables_dir_file_name}
echo "# [${process_id}]	Successfully ran ${create_wide_tables_dir_file_name} to create dirs for external partitions"

sudo -u hive hive -S -f ${create_wide_tables_file_name}
validate_ret $?
echo "# [${process_id}]	Successfully ran bulk file ${create_wide_tables_file_name} to create wide tables with external partitions"

sudo -u hive ${BL} -f ${create_db_role_file_name} 1>/dev/null # to remove white lines after execution
validate_ret $?
echo "# [${process_id}]	Successfully created database level roles and privileges"

sudo -u hive ${BL} -f ${create_tbl_role_file_name} 1>/dev/null # to remove white lines after execution
validate_ret $?
echo "# [${process_id}]	Successfully created table level roles and privileges"

res_file=${LOCAL_OUTPUT_DIR}/hive_${prefix_string}.res
echo "-- [${process_id}]	List all databases and roles in ${res_file}" > ${res_file}
sudo -u hive  ${BL} -e "show databases" 2>/dev/null 1>>${res_file}
sudo -u hive  ${BL} -e "show roles" 2>/dev/null 1>>${res_file}
echo "[${process_id}]	Successfully listed all databases and roles in ${res_file}"
