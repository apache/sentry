-- create index for foreign key AUTHZ_OBJ_ID
set serveroutput on
declare
  already_exists  exception;
  columns_indexed exception;
  pragma exception_init( already_exists, -955 );
  pragma exception_init(columns_indexed, -1408);
begin
  execute immediate 'CREATE INDEX "AUTHZ_PATH_FK_IDX" ON "AUTHZ_PATH" ("AUTHZ_OBJ_ID")';
  dbms_output.put_line( 'created' );
exception
  when already_exists or columns_indexed then
  dbms_output.put_line( 'skipped' );
end;
/