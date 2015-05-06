SELECT 'Upgrading Sentry store schema from 1.4.0 to 1.4.0-cdh5' AS ' ';
SOURCE 001-SENTRY-327.mysql.sql;
SOURCE 002-SENTRY-339.mysql.sql;
SOURCE 003-SENTRY-380.mysql.sql;
SOURCE 004-SENTRY-BUGFIX.mysql.sql;

UPDATE SENTRY_VERSION SET SCHEMA_VERSION='1.4.0-cdh5', VERSION_COMMENT='Sentry release version 1.4.0-cdh5' WHERE VER_ID=1;
SELECT 'Finish upgrading Sentry store schema from 1.4.0 to 1.4.0-cdh5' AS ' ';

