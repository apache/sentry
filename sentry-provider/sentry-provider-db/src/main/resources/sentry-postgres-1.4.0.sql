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
  "PRIVILEGE_NAME" character varying(128) NOT NULL, -- Name of the privilege
  "PRIVILEGE_SCOPE" character varying(32) NOT NULL, -- Scope. Valid values are Server, Database, Table
  "SERVER_NAME" character varying(128) NOT NULL,
  "DATABASE_NAME" character varying(128) DEFAULT NULL::character varying,
  "TABLE_NAME" character varying(128) DEFAULT NULL::character varying,
  "URI" character varying(4000) DEFAULT NULL::character varying,
  "PRIVILEGE" character varying(128) NOT NULL, -- Allowed action. Valid values are ALL, INSERT, SELECT
  "CREATE_TIME" BIGINT NOT NULL,
  "GRANTOR_PRINCIPAL" VARCHAR(128) NOT NULL -- principal of the creator
);

CREATE TABLE "SENTRY_ROLE" (
  "ROLE_ID" BIGINT  NOT NULL,
  "ROLE_NAME" character varying(128) NOT NULL,
  "CREATE_TIME" BIGINT NOT NULL,
  "ROLE_OWNER" character varying(128) NOT NULL
);

CREATE TABLE "SENTRY_GROUP" (
  "GROUP_ID" BIGINT  NOT NULL,
  "GROUP_NAME" character varying(128) NOT NULL,
  "CREATE_TIME" BIGINT NOT NULL,
  "GRANTOR_PRINCIPAL" character varying(128) NOT NULL
);

CREATE TABLE "SENTRY_ROLE_DB_PRIVILEGE_MAP" (
  "ROLE_PRIVILEGE_MAP_ID" BIGINT NOT NULL,
  "ROLE_ID" BIGINT NOT NULL, -- FK to SENTRY_ROLE.ROLE_ID
  "DB_PRIVILEGE_ID" BIGINT NOT NULL -- FK to SENTRY_DB_PRIVILEGE.DB_PRIVILEGE_ID
);

CREATE TABLE "SENTRY_ROLE_GROUP_MAP" (
  "ROLE_GROUP_MAP_ID" BIGINT NOT NULL,
  "ROLE_ID" BIGINT NOT NULL, -- FK to SENTRY_ROLE.ROLE_ID
  "GROUP_ID" BIGINT NOT NULL -- FK to SENTRY_GROUP.GROUP_ID
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

ALTER TABLE ONLY "SENTRY_ROLE_DB_PRIVILEGE_MAP"
  ADD CONSTRAINT "SENTRY_ROLE_DB_PRIV_MAP_PK" PRIMARY KEY ("ROLE_PRIVILEGE_MAP_ID");

ALTER TABLE ONLY "SENTRY_ROLE_GROUP_MAP"
  ADD CONSTRAINT "SENTRY_ROLE_GROUP_MAP_PK" PRIMARY KEY ("ROLE_GROUP_MAP_ID");

ALTER TABLE ONLY "SENTRY_VERSION" ADD CONSTRAINT "SENTRY_VERSION_PK" PRIMARY KEY ("VER_ID");

ALTER TABLE ONLY "SENTRY_DB_PRIVILEGE"
  ADD CONSTRAINT "SENTRY_DB_PRIV_PRIV_NAME_UNIQ" UNIQUE ("PRIVILEGE_NAME");

ALTER TABLE ONLY "SENTRY_ROLE"
  ADD CONSTRAINT "SENTRY_ROLE_ROLE_NAME_UNIQUE" UNIQUE ("ROLE_NAME");

ALTER TABLE ONLY "SENTRY_GROUP"
  ADD CONSTRAINT "SENTRY_GRP_GRP_NAME_UNIQUE" UNIQUE ("GROUP_NAME");

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

INSERT INTO "SENTRY_VERSION" ("VER_ID", "SCHEMA_VERSION", "VERSION_COMMENT") VALUES (1, '1.4.0', 'Sentry release version 1.4.0');

COMMIT;
