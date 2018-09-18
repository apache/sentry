SELECT 'Upgrading Sentry store schema from 2.1.0 to 2.2.0';


UPDATE "SENTRY_VERSION" SET "SCHEMA_VERSION"='2.2.0', "VERSION_COMMENT"='Sentry release version 2.2.0' WHERE "VER_ID"=1;

SELECT 'Finished upgrading Sentry store schema from 2.1.0 to 2.2.0';