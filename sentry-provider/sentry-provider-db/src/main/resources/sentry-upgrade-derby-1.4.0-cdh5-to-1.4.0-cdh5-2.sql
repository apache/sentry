RUN '004-SENTRY-BUGFIX.derby.sql';
-- Version update
UPDATE SENTRY_VERSION SET SCHEMA_VERSION='1.4.0-cdh5-2', VERSION_COMMENT='Sentry release version 1.4.0-cdh5-2' WHERE VER_ID=1;
