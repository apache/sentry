--Licensed to the Apache Software Foundation (ASF) under one or more
--contributor license agreements.  See the NOTICE file distributed with
--this work for additional information regarding copyright ownership.
--The ASF licenses this file to You under the Apache License, Version 2.0
--(the "License"); you may not use this file except in compliance with
--the License.  You may obtain a copy of the License at
--
--    http://www.apache.org/licenses/LICENSE-2.0
--
--Unless required by applicable law or agreed to in writing, software
--distributed under the License is distributed on an "AS IS" BASIS,
--WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
--See the License for the specific language governing permissions and
--limitations under the License.

START TRANSACTION;

SET statement_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = off;
SET check_function_bodies = false;
SET client_min_messages = warning;
SET escape_string_warning = off;
SET search_path = public, pg_catalog;
SET default_tablespace = '';
SET default_with_oids = false;

CREATE TABLE "SENTRY_DB_PRIVILEGE" (
  "DB_PRIVILEGE_ID" BIGINT NOT NULL,
  "PRIVILEGE_SCOPE" character varying(32) NOT NULL,
  "SERVER_NAME" character varying(128) NOT NULL,
  "DB_NAME" character varying(128) DEFAULT '__NULL__',
  "TABLE_NAME" character varying(128) DEFAULT '__NULL__',
  "COLUMN_NAME" character varying(128) DEFAULT '__NULL__',
  "URI" character varying(4000) DEFAULT '__NULL__',
  "ACTION" character varying(128) NOT NULL,
  "CREATE_TIME" BIGINT NOT NULL,
  "WITH_GRANT_OPTION" CHAR(1) NOT NULL
);

CREATE TABLE "SENTRY_ROLE" (
  "ROLE_ID" BIGINT  NOT NULL,
  "ROLE_NAME" character varying(128) NOT NULL,
  "CREATE_TIME" BIGINT NOT NULL
);

CREATE TABLE "SENTRY_GROUP" (
  "GROUP_ID" BIGINT  NOT NULL,
  "GROUP_NAME" character varying(128) NOT NULL,
  "CREATE_TIME" BIGINT NOT NULL
);

CREATE TABLE "SENTRY_ROLE_DB_PRIVILEGE_MAP" (
  "ROLE_ID" BIGINT NOT NULL,
  "DB_PRIVILEGE_ID" BIGINT NOT NULL,
  "GRANTOR_PRINCIPAL" character varying(128)
);

CREATE TABLE "SENTRY_ROLE_GROUP_MAP" (
  "ROLE_ID" BIGINT NOT NULL,
  "GROUP_ID" BIGINT NOT NULL,
  "GRANTOR_PRINCIPAL" character varying(128)
);

CREATE TABLE "SENTRY_VERSION" (
  "VER_ID" bigint,
  "SCHEMA_VERSION" character varying(127) NOT NULL,
  "VERSION_COMMENT" character varying(255) NOT NULL
);


ALTER TABLE ONLY "SENTRY_DB_PRIVILEGE"
  ADD CONSTRAINT "SENTRY_DB_PRIV_PK" PRIMARY KEY ("DB_PRIVILEGE_ID");

ALTER TABLE ONLY "SENTRY_ROLE"
  ADD CONSTRAINT "SENTRY_ROLE_PK" PRIMARY KEY ("ROLE_ID");

ALTER TABLE ONLY "SENTRY_GROUP"
  ADD CONSTRAINT "SENTRY_GROUP_PK" PRIMARY KEY ("GROUP_ID");

ALTER TABLE ONLY "SENTRY_VERSION" ADD CONSTRAINT "SENTRY_VERSION_PK" PRIMARY KEY ("VER_ID");

ALTER TABLE ONLY "SENTRY_DB_PRIVILEGE"
  ADD CONSTRAINT "SENTRY_DB_PRIV_PRIV_NAME_UNIQ" UNIQUE ("SERVER_NAME","DB_NAME","TABLE_NAME","COLUMN_NAME","URI", "ACTION","WITH_GRANT_OPTION");

CREATE INDEX "SENTRY_PRIV_SERV_IDX" ON "SENTRY_DB_PRIVILEGE" USING btree ("SERVER_NAME");

CREATE INDEX "SENTRY_PRIV_DB_IDX" ON "SENTRY_DB_PRIVILEGE" USING btree ("DB_NAME");

CREATE INDEX "SENTRY_PRIV_TBL_IDX" ON "SENTRY_DB_PRIVILEGE" USING btree ("TABLE_NAME");

CREATE INDEX "SENTRY_PRIV_COL_IDX" ON "SENTRY_DB_PRIVILEGE" USING btree ("COLUMN_NAME");

CREATE INDEX "SENTRY_PRIV_URI_IDX" ON "SENTRY_DB_PRIVILEGE" USING btree ("URI");

ALTER TABLE ONLY "SENTRY_ROLE"
  ADD CONSTRAINT "SENTRY_ROLE_ROLE_NAME_UNIQUE" UNIQUE ("ROLE_NAME");

ALTER TABLE ONLY "SENTRY_GROUP"
  ADD CONSTRAINT "SENTRY_GRP_GRP_NAME_UNIQUE" UNIQUE ("GROUP_NAME");

ALTER TABLE "SENTRY_ROLE_DB_PRIVILEGE_MAP"
  ADD CONSTRAINT "SENTRY_ROLE_DB_PRIVILEGE_MAP_PK" PRIMARY KEY ("ROLE_ID","DB_PRIVILEGE_ID");

ALTER TABLE "SENTRY_ROLE_GROUP_MAP"
  ADD CONSTRAINT "SENTRY_ROLE_GROUP_MAP_PK" PRIMARY KEY ("ROLE_ID","GROUP_ID");

ALTER TABLE ONLY "SENTRY_ROLE_DB_PRIVILEGE_MAP"
  ADD CONSTRAINT "SEN_RLE_DB_PRV_MAP_SN_RLE_FK"
  FOREIGN KEY ("ROLE_ID") REFERENCES "SENTRY_ROLE"("ROLE_ID") DEFERRABLE;

ALTER TABLE ONLY "SENTRY_ROLE_DB_PRIVILEGE_MAP"
  ADD CONSTRAINT "SEN_RL_DB_PRV_MAP_SN_DB_PRV_FK"
  FOREIGN KEY ("DB_PRIVILEGE_ID") REFERENCES "SENTRY_DB_PRIVILEGE"("DB_PRIVILEGE_ID") DEFERRABLE;

ALTER TABLE ONLY "SENTRY_ROLE_GROUP_MAP"
  ADD CONSTRAINT "SEN_ROLE_GROUP_MAP_SEN_ROLE_FK"
  FOREIGN KEY ("ROLE_ID") REFERENCES "SENTRY_ROLE"("ROLE_ID") DEFERRABLE;

ALTER TABLE ONLY "SENTRY_ROLE_GROUP_MAP"
  ADD CONSTRAINT "SEN_ROLE_GROUP_MAP_SEN_GRP_FK"
  FOREIGN KEY ("GROUP_ID") REFERENCES "SENTRY_GROUP"("GROUP_ID") DEFERRABLE;

INSERT INTO "SENTRY_VERSION" ("VER_ID", "SCHEMA_VERSION", "VERSION_COMMENT") VALUES (1, '1.5.0-cdh5', 'Sentry release version 1.5.0-cdh5');

-- Generic Model
-- Table SENTRY_GM_PRIVILEGE for classes [org.apache.sentry.provider.db.service.model.MSentryGMPrivilege]
CREATE TABLE "SENTRY_GM_PRIVILEGE" (
  "GM_PRIVILEGE_ID" BIGINT NOT NULL,
  "COMPONENT_NAME" character varying(32) NOT NULL,
  "SERVICE_NAME" character varying(64) NOT NULL,
  "RESOURCE_NAME_0" character varying(64) DEFAULT '__NULL__',
  "RESOURCE_NAME_1" character varying(64) DEFAULT '__NULL__',
  "RESOURCE_NAME_2" character varying(64) DEFAULT '__NULL__',
  "RESOURCE_NAME_3" character varying(64) DEFAULT '__NULL__',
  "RESOURCE_TYPE_0" character varying(64) DEFAULT '__NULL__',
  "RESOURCE_TYPE_1" character varying(64) DEFAULT '__NULL__',
  "RESOURCE_TYPE_2" character varying(64) DEFAULT '__NULL__',
  "RESOURCE_TYPE_3" character varying(64) DEFAULT '__NULL__',
  "ACTION" character varying(32) NOT NULL,
  "SCOPE" character varying(128) NOT NULL,
  "CREATE_TIME" BIGINT NOT NULL,
  "WITH_GRANT_OPTION" CHAR(1) NOT NULL
);
ALTER TABLE ONLY "SENTRY_GM_PRIVILEGE"
  ADD CONSTRAINT "SENTRY_GM_PRIV_PK" PRIMARY KEY ("GM_PRIVILEGE_ID");
-- Constraints for table SENTRY_GM_PRIVILEGE for class(es) [org.apache.sentry.provider.db.service.model.MSentryGMPrivilege]
ALTER TABLE ONLY "SENTRY_GM_PRIVILEGE"
  ADD CONSTRAINT "SENTRY_GM_PRIV_PRIV_NAME_UNIQ" UNIQUE ("COMPONENT_NAME","SERVICE_NAME","RESOURCE_NAME_0","RESOURCE_NAME_1","RESOURCE_NAME_2",
  "RESOURCE_NAME_3","RESOURCE_TYPE_0","RESOURCE_TYPE_1","RESOURCE_TYPE_2","RESOURCE_TYPE_3","ACTION","WITH_GRANT_OPTION");

CREATE INDEX "SENTRY_GM_PRIV_COMP_IDX" ON "SENTRY_GM_PRIVILEGE" USING btree ("COMPONENT_NAME");

CREATE INDEX "SENTRY_GM_PRIV_SERV_IDX" ON "SENTRY_GM_PRIVILEGE" USING btree ("SERVICE_NAME");

CREATE INDEX "SENTRY_GM_PRIV_RES0_IDX" ON "SENTRY_GM_PRIVILEGE" USING btree ("RESOURCE_NAME_0","RESOURCE_TYPE_0");

CREATE INDEX "SENTRY_GM_PRIV_RES1_IDX" ON "SENTRY_GM_PRIVILEGE" USING btree ("RESOURCE_NAME_1","RESOURCE_TYPE_1");

CREATE INDEX "SENTRY_GM_PRIV_RES2_IDX" ON "SENTRY_GM_PRIVILEGE" USING btree ("RESOURCE_NAME_2","RESOURCE_TYPE_2");

CREATE INDEX "SENTRY_GM_PRIV_RES3_IDX" ON "SENTRY_GM_PRIVILEGE" USING btree ("RESOURCE_NAME_3","RESOURCE_TYPE_3");

-- Table SENTRY_ROLE_GM_PRIVILEGE_MAP for join relationship
CREATE TABLE "SENTRY_ROLE_GM_PRIVILEGE_MAP" (
  "ROLE_ID" BIGINT NOT NULL,
  "GM_PRIVILEGE_ID" BIGINT NOT NULL
);

ALTER TABLE "SENTRY_ROLE_GM_PRIVILEGE_MAP"
  ADD CONSTRAINT "SENTRY_ROLE_GM_PRIVILEGE_MAP_PK" PRIMARY KEY ("ROLE_ID","GM_PRIVILEGE_ID");

-- Constraints for table SENTRY_ROLE_GM_PRIVILEGE_MAP
ALTER TABLE ONLY "SENTRY_ROLE_GM_PRIVILEGE_MAP"
  ADD CONSTRAINT "SEN_RLE_GM_PRV_MAP_SN_RLE_FK"
  FOREIGN KEY ("ROLE_ID") REFERENCES "SENTRY_ROLE"("ROLE_ID") DEFERRABLE;

ALTER TABLE ONLY "SENTRY_ROLE_GM_PRIVILEGE_MAP"
  ADD CONSTRAINT "SEN_RL_GM_PRV_MAP_SN_DB_PRV_FK"
  FOREIGN KEY ("GM_PRIVILEGE_ID") REFERENCES "SENTRY_GM_PRIVILEGE"("GM_PRIVILEGE_ID") DEFERRABLE;

-- Table "AUTHZ_PATHS_MAPPING" for classes [org.apache.sentry.provider.db.service.model.MAuthzPathsMapping]
CREATE TABLE "AUTHZ_PATHS_MAPPING"
(
    "AUTHZ_OBJ_ID" SERIAL,
    "AUTHZ_OBJ_NAME" varchar(384) NULL,
    "CREATE_TIME_MS" int8 NOT NULL,
    CONSTRAINT "AUTHZ_PATHS_MAPPING_PK" PRIMARY KEY ("AUTHZ_OBJ_ID")
);

-- Table "MAUTHZPATHSMAPPING_PATHS" for join relationship
CREATE TABLE "MAUTHZPATHSMAPPING_PATHS"
(
    "AUTHZ_OBJ_ID_OID" int8 NOT NULL,
    "PATHS" varchar(4000) NOT NULL,
    CONSTRAINT "MAUTHZPATHSMAPPING_PATHS_PK" PRIMARY KEY ("AUTHZ_OBJ_ID_OID","PATHS")
);

-- Constraints for table "AUTHZ_PATHS_MAPPING" for class(es) [org.apache.sentry.provider.db.service.model.MAuthzPathsMapping]
CREATE UNIQUE INDEX "AUTHZOBJNAME" ON "AUTHZ_PATHS_MAPPING" ("AUTHZ_OBJ_NAME");

-- Constraints for table "MAUTHZPATHSMAPPING_PATHS"
ALTER TABLE "MAUTHZPATHSMAPPING_PATHS" ADD CONSTRAINT "MAUTHZPATHSMAPPING_PATHS_FK1" FOREIGN KEY ("AUTHZ_OBJ_ID_OID") REFERENCES "AUTHZ_PATHS_MAPPING" ("AUTHZ_OBJ_ID") INITIALLY DEFERRED ;

CREATE INDEX "MAUTHZPATHSMAPPING_PATHS_N49" ON "MAUTHZPATHSMAPPING_PATHS" ("AUTHZ_OBJ_ID_OID");

COMMIT;