SELECT 'Upgrading Sentry store schema from 1.4.0 to 1.5.0';
\i 004-SENTRY-74.postgres.sql;
\i 005-SENTRY-398.postgres.sql;

UPDATE "SENTRY_VERSION" SET "SCHEMA_VERSION"='1.5.0', "VERSION_COMMENT"='Sentry release version 1.5.0' WHERE "VER_ID"=1;
SELECT 'Finished upgrading Sentry store schema from 1.4.0 to 1.5.0';
