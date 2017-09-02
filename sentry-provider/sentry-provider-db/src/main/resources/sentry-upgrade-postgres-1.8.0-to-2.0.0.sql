SELECT 'Upgrading Sentry store schema from 1.8.0 to 2.0.0';
\i 007-SENTRY-1365.postgres.sql;
\i 008-SENTRY-1569.postgres.sql;
\i 009-SENTRY-1805.postgres.sql;

UPDATE "SENTRY_VERSION" SET "SCHEMA_VERSION"='2.0.0', "VERSION_COMMENT"='Sentry release version 2.0.0' WHERE "VER_ID"=1;

SELECT 'Finished upgrading Sentry store schema from 1.8.0 to 2.0.0';
