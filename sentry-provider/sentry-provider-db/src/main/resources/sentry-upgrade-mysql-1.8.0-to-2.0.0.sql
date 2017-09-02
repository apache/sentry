SELECT 'Upgrading Sentry store schema from 1.8.0 to 2.0.0' AS ' ';
SOURCE 007-SENTRY-1365.mysql.sql;
SOURCE 008-SENTRY-1569.mysql.sql;
SOURCE 009-SENTRY-1805.mysql.sql;

UPDATE SENTRY_VERSION SET SCHEMA_VERSION='2.0.0', VERSION_COMMENT='Sentry release version 2.0.0' WHERE VER_ID=1;

SELECT 'Finish upgrading Sentry store schema from 1.8.0 to 2.0.0' AS ' ';
