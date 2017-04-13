-- Table SENTRY_PERM_CHANGE for classes [org.apache.sentry.provider.db.service.model.MSentryPermChange]
CREATE TABLE SENTRY_PERM_CHANGE
(
    CHANGE_ID BIGINT NOT NULL,
    CREATE_TIME_MS BIGINT NOT NULL,
    PERM_CHANGE VARCHAR(4000) NOT NULL
);

ALTER TABLE SENTRY_PERM_CHANGE ADD CONSTRAINT SENTRY_PERM_CHANGE_PK PRIMARY KEY (CHANGE_ID);

-- Table SENTRY_PATH_CHANGE for classes [org.apache.sentry.provider.db.service.model.MSentryPathChange]
CREATE TABLE SENTRY_PATH_CHANGE
(
    CHANGE_ID BIGINT NOT NULL,
    NOTIFICATION_ID BIGINT NOT NULL,
    CREATE_TIME_MS BIGINT NOT NULL,
    PATH_CHANGE VARCHAR(4000) NOT NULL
);

-- Constraints for table SENTRY_PATH_CHANGE for class [org.apache.sentry.provider.db.service.model.MSentryPathChange]
ALTER TABLE SENTRY_PATH_CHANGE ADD CONSTRAINT SENTRY_PATH_CHANGE_PK PRIMARY KEY (CHANGE_ID);

CREATE UNIQUE INDEX NOTIFICATIONID ON SENTRY_PATH_CHANGE (NOTIFICATION_ID);