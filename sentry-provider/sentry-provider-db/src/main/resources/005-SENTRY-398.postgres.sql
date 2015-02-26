-- Table SENTRY_GM_PRIVILEGE for classes [org.apache.sentry.provider.db.service.model.MSentryGMPrivilege]
CREATE TABLE "SENTRY_GM_PRIVILEGE" (
  "GM_PRIVILEGE_ID" BIGINT NOT NULL,
  "COMPONENT_NAME" character varying(32) NOT NULL,
  "SERVICE_NAME" character varying(64) NOT NULL,
  "RESOURCE_NAME_0" character varying(64) DEFAULT NULL::character varying,
  "RESOURCE_NAME_1" character varying(64) DEFAULT NULL::character varying,
  "RESOURCE_NAME_2" character varying(64) DEFAULT NULL::character varying,
  "RESOURCE_NAME_3" character varying(64) DEFAULT NULL::character varying,
  "RESOURCE_TYPE_0" character varying(64) DEFAULT NULL::character varying,
  "RESOURCE_TYPE_1" character varying(64) DEFAULT NULL::character varying,
  "RESOURCE_TYPE_2" character varying(64) DEFAULT NULL::character varying,
  "RESOURCE_TYPE_3" character varying(64) DEFAULT NULL::character varying,
  "ACTION" character varying(32) NOT NULL,
  "scope" character varying(128) NOT NULL,
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