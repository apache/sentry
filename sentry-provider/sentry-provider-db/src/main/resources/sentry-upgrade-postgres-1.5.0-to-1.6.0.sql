SELECT 'Upgrading Sentry store schema from 1.5.0 to 1.6.0';

UPDATE "SENTRY_VERSION" SET "SCHEMA_VERSION"='1.6.0', "VERSION_COMMENT"='Sentry release version 1.6.0' WHERE "VER_ID"=1;

SELECT 'Finished upgrading Sentry store schema from 1.5.0 to 1.6.0';