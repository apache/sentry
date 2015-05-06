SELECT 'Upgrading Sentry store schema from 1.4.0 to 1.4.0-cdh5' AS Status from dual;
@001-SENTRY-327.oracle.sql;
@002-SENTRY-339.oracle.sql;
@003-SENTRY-380.oracle.sql;
@004-SENTRY-BUGFIX.oracle.sql;

UPDATE SENTRY_VERSION SET SCHEMA_VERSION='1.4.0-cdh5', VERSION_COMMENT='Sentry release version 1.4.0-cdh5' WHERE VER_ID=1;
SELECT 'Finished upgrading Sentry store schema from 1.4.0 to 1.4.0-cdh5' AS Status from dual;
