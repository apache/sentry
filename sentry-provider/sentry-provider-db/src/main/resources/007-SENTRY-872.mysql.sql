-- Table `AUTHZ_PATHS_MAPPING` for classes [org.apache.sentry.provider.db.service.model.MAuthzPathsMapping]
CREATE TABLE `AUTHZ_PATHS_MAPPING`
(
    `AUTHZ_OBJ_ID` BIGINT NOT NULL AUTO_INCREMENT,
    `AUTHZ_OBJ_NAME` VARCHAR(384) BINARY CHARACTER SET latin1 COLLATE latin1_bin NOT NULL,
    `CREATE_TIME_MS` BIGINT NOT NULL,
    CONSTRAINT `AUTHZ_PATHS_MAPPING_PK` PRIMARY KEY (`AUTHZ_OBJ_ID`)
) ENGINE=INNODB;

-- Constraints for table `AUTHZ_PATHS_MAPPING` for class(es) [org.apache.sentry.provider.db.service.model.MAuthzPathsMapping]
CREATE UNIQUE INDEX `AUTHZOBJNAME` ON `AUTHZ_PATHS_MAPPING` (`AUTHZ_OBJ_NAME`);

-- Table `AUTHZ_PATH` for classes [org.apache.sentry.provider.db.service.model.MPath]
CREATE TABLE `AUTHZ_PATH` (
    `PATH_ID` BIGINT NOT NULL,
    `PATH_NAME` VARCHAR(4000) CHARACTER SET utf8 COLLATE utf8_bin,
    `AUTHZ_OBJ_ID` BIGINT
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- Constraints for table `AUTHZ_PATH`
ALTER TABLE `AUTHZ_PATH`
  ADD CONSTRAINT `AUTHZ_PATH_PK` PRIMARY KEY (`PATH_ID`);

ALTER TABLE `AUTHZ_PATH`
  ADD CONSTRAINT `AUTHZ_PATH_FK`
  FOREIGN KEY (`AUTHZ_OBJ_ID`) REFERENCES `AUTHZ_PATHS_MAPPING`(`AUTHZ_OBJ_ID`);

------------------------------------------------------------------
-- Sequences and SequenceTables
