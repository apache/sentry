RUN '001-SENTRY-327.derby.sql';
RUN '002-SENTRY-339.derby.sql';
RUN '003-SENTRY-380.derby.sql';
RUN '004-SENTRY-74.derby.sql';
RUN '005-SENTRY-398.derby.sql';

-- Version update
UPDATE SENTRY_VERSION SET SCHEMA_VERSION='1.5.0', VERSION_COMMENT='Sentry release version 1.5.0' WHERE VER_ID=1;
