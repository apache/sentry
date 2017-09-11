-- Table `SENTRY_PERM_CHANGE` for classes [org.apache.sentry.provider.db.service.model.MSentryPermChange]
CREATE TABLE "SENTRY_PERM_CHANGE"
(
    "CHANGE_ID" bigint NOT NULL,
    "CREATE_TIME_MS" bigint NOT NULL,
    "PERM_CHANGE" VARCHAR(4000) NOT NULL,
    CONSTRAINT "SENTRY_PERM_CHANGE_PK" PRIMARY KEY ("CHANGE_ID")
);

-- Table `SENTRY_PATH_CHANGE` for classes [org.apache.sentry.provider.db.service.model.MSentryPathChange]
CREATE TABLE "SENTRY_PATH_CHANGE"
(
    "CHANGE_ID" bigint NOT NULL,
    "NOTIFICATION_HASH" char(40) NOT NULL,
    "CREATE_TIME_MS" bigint NOT NULL,
    "PATH_CHANGE" text NOT NULL,
    CONSTRAINT "SENTRY_PATH_CHANGE_PK" PRIMARY KEY ("CHANGE_ID")
);

-- Constraints for table SENTRY_PATH_CHANGE for class [org.apache.sentry.provider.db.service.model.MSentryPathChange]
CREATE UNIQUE INDEX "NOTIFICATION_HASH_INDEX" ON "SENTRY_PATH_CHANGE" ("NOTIFICATION_HASH");

-- Table SENTRY_HMS_NOTIFICATION_ID for classes [org.apache.sentry.provider.db.service.model.MSentryHmsNotification]
CREATE TABLE "SENTRY_HMS_NOTIFICATION_ID"
(
    "NOTIFICATION_ID" bigint NOT NULL
);

CREATE INDEX "SENTRY_HMS_NOTIF_ID_INDEX" ON "SENTRY_HMS_NOTIFICATION_ID" ("NOTIFICATION_ID");