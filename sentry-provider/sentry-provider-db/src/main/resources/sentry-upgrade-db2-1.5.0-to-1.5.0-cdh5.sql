-- SENTRY-872
-- Table AUTHZ_PATHS_MAPPING for classes [org.apache.sentry.provider.db.service.model.MAuthzPathsMapping]
 CREATE TABLE AUTHZ_PATHS_MAPPING(AUTHZ_OBJ_ID BIGINT NOT NULL generated always as identity (start with 1),AUTHZ_OBJ_NAME VARCHAR(384),CREATE_TIME_MS BIGINT NOT NULL);

 ALTER TABLE AUTHZ_PATHS_MAPPING ADD CONSTRAINT AUTHZ_PATHSCO7K_PK PRIMARY KEY (AUTHZ_OBJ_ID);

-- Table MAUTHZPATHSMAPPING_PATHS for join relationship
 CREATE TABLE MAUTHZPATHSMAPPING_PATHS(AUTHZ_OBJ_ID_OID BIGINT NOT NULL,PATHS VARCHAR(4000) NOT NULL);

 ALTER TABLE MAUTHZPATHSMAPPING_PATHS ADD CONSTRAINT MAUTHZPATHSS184_PK PRIMARY KEY (AUTHZ_OBJ_ID_OID,PATHS);

-- Constraints for table AUTHZ_PATHS_MAPPING for class(es) [org.apache.sentry.provider.db.service.model.MAuthzPathsMapping]
 CREATE UNIQUE INDEX AUTHZOBJNAME ON AUTHZ_PATHS_MAPPING (AUTHZ_OBJ_NAME);

-- Constraints for table MAUTHZPATHSMAPPING_PATHS
 ALTER TABLE MAUTHZPATHSMAPPING_PATHS ADD CONSTRAINT MAUTHZPATHS184_FK1 FOREIGN KEY (AUTHZ_OBJ_ID_OID) REFERENCES AUTHZ_PATHS_MAPPING (AUTHZ_OBJ_ID) ;

 CREATE INDEX MAUTHZPATHS184_N49 ON MAUTHZPATHSMAPPING_PATHS (AUTHZ_OBJ_ID_OID);

-- Version update
UPDATE SENTRY_VERSION SET SCHEMA_VERSION='1.5.0-cdh5', VERSION_COMMENT='Sentry release version 1.5.0-cdh5' WHERE VER_ID=1;