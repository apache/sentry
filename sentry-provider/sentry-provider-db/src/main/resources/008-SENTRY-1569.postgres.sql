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
    "NOTIFICATION_ID" bigint NOT NULL,
    "CREATE_TIME_MS" bigint NOT NULL,
    "PATH_CHANGE" text NOT NULL,
    CONSTRAINT "SENTRY_PATH_CHANGE_PK" PRIMARY KEY ("CHANGE_ID")
);

-- Table SENTRY_HMS_NOTIFICATION_ID for classes [org.apache.sentry.provider.db.service.model.MSentryHmsNotification]
CREATE TABLE "SENTRY_HMS_NOTIFICATION_ID"
(
    "NOTIFICATION_ID" bigint NOT NULL
);
