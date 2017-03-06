SELECT 'Upgrading Sentry store schema from 1.5.0 to 1.5.0-cdh5' AS ' ';
SOURCE 007-SENTRY-872.mysql.sql;
SOURCE 008-SENTRY-1569.mysql.sql;

UPDATE SENTRY_VERSION SET SCHEMA_VERSION='1.5.0-cdh5', VERSION_COMMENT='Sentry release version 1.5.0-cdh5' WHERE VER_ID=1;
SELECT 'Finish upgrading Sentry store schema from 1.5.0 to 1.5.0-cdh5' AS ' ';

