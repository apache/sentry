SELECT 'Upgrading Sentry store schema from 1.4.0 to 1.4.0-cdh5';
\i 001-SENTRY-327.postgres.sql;
\i 002-SENTRY-339.postgres.sql;
\i 003-SENTRY-380.postgres.sql;
\i 004-SENTRY-BUGFIX.postgres.sql;

UPDATE "SENTRY_VERSION" SET "SCHEMA_VERSION"='1.4.0-cdh5', "VERSION_COMMENT"='Sentry release version 1.4.0-cdh5' WHERE "VER_ID"=1;
SELECT 'Finished upgrading Sentry store schema from 1.4.0 to 1.4.0-cdh5';
