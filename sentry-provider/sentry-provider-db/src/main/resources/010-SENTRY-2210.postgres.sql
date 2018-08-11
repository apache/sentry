-- create index for foreign key AUTHZ_OBJ_ID
DO $BLOCK$
BEGIN
    BEGIN
        CREATE INDEX "AUTHZ_PATH_FK_IDX" ON "AUTHZ_PATH"( "AUTHZ_OBJ_ID" );
    EXCEPTION
        WHEN duplicate_table
        THEN RAISE NOTICE 'index ''AUTHZ_PATH_FK_IDX '' on ''AUTHZ_PATH'' already exists, skipping';
    END;
END;
$BLOCK$;