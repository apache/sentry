-- Table AUTHZ_PATHS_SNAPSHOT_ID for class [org.apache.sentry.provider.db.service.model.MAuthzPathsSnapshotId]
CREATE TABLE AUTHZ_PATHS_SNAPSHOT_ID
(
    AUTHZ_SNAPSHOT_ID BIGINT NOT NULL
);

-- Constraints for table AUTHZ_PATHS_SNAPSHOT_ID for class [org.apache.sentry.provider.db.service.model.MAuthzPathsSnapshotId]
ALTER TABLE AUTHZ_PATHS_SNAPSHOT_ID ADD CONSTRAINT AUTHZ_SNAPSHOT_ID_PK PRIMARY KEY (AUTHZ_SNAPSHOT_ID);