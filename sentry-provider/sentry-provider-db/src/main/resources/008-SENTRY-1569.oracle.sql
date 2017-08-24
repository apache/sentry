-- Table "SENTRY_PERM_CHANGE" for classes [org.apache.sentry.provider.db.service.model.MSentryPermChange]
CREATE TABLE "SENTRY_PERM_CHANGE"
(
    "CHANGE_ID" NUMBER NOT NULL,
    "CREATE_TIME_MS" NUMBER NOT NULL,
    "PERM_CHANGE" VARCHAR2(4000) NOT NULL
);

ALTER TABLE "SENTRY_PERM_CHANGE" ADD CONSTRAINT "SENTRY_PERM_CHANGE_PK" PRIMARY KEY ("CHANGE_ID");

-- Table "SENTRY_PATH_CHANGE" for classes [org.apache.sentry.provider.db.service.model.MSentryPathChange]
CREATE TABLE "SENTRY_PATH_CHANGE"
(
    "CHANGE_ID" NUMBER NOT NULL,
    "NOTIFICATION_ID" NUMBER NOT NULL,
    "CREATE_TIME_MS" NUMBER NOT NULL,
    "PATH_CHANGE" CLOB NOT NULL
);

-- Constraints for table SENTRY_PATH_CHANGE for class [org.apache.sentry.provider.db.service.model.MSentryPathChange]
ALTER TABLE "SENTRY_PATH_CHANGE" ADD CONSTRAINT SENTRY_PATH_CHANGE_PK PRIMARY KEY ("CHANGE_ID");

-- Table SENTRY_HMS_NOTIFICATION_ID for classes [org.apache.sentry.provider.db.service.model.MSentryHmsNotification]
CREATE TABLE "SENTRY_HMS_NOTIFICATION_ID"
(
    "NOTIFICATION_ID" NUMBER NOT NULL
);