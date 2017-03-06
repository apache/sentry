SELECT 'Upgrading Sentry store schema from 1.5.0 to 1.5.0-cdh5' AS Status from dual;
@007-SENTRY-872.oracle.sql;
@008-SENTRY-1569.oracle.sql;

UPDATE SENTRY_VERSION SET SCHEMA_VERSION='1.5.0-cdh5', VERSION_COMMENT='Sentry release version 1.5.0-cdh5' WHERE VER_ID=1;
SELECT 'Finished upgrading Sentry store schema from 1.5.0 to 1.5.0-cdh5' AS Status from dual;
