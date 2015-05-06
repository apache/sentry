SELECT 'Upgrading Sentry store schema from 1.4.0-cdh5 to 1.4.0-cdh5-2' AS ' ';
SOURCE 004-SENTRY-BUGFIX.mysql.sql;

UPDATE SENTRY_VERSION SET SCHEMA_VERSION='1.4.0-cdh5-2', VERSION_COMMENT='Sentry release version 1.4.0-cdh5-2' WHERE VER_ID=1;
SELECT 'Finish upgrading Sentry store schema from 1.4.0-cdh5 to 1.4.0-cdh5-2' AS ' ';

