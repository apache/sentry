RUN '001-SENTRY-327.derby.sql';
RUN '002-SENTRY-339.derby.sql';

-- Version update
UPDATE SENTRY_VERSION SET SCHEMA_VERSION='1.5.0', VERSION_COMMENT='Sentry release version 1.5.0' WHERE VER_ID=1;
