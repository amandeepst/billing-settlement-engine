begin
  pkg_privs.pr_grant_object_privs(pkg_opr.BILLING_AND_FUNDING);
end;
/
begin
  pkg_synonyms.pr_create_synonyms(pkg_opr.BILLING_AND_FUNDING);
end;
/
BEGIN
  FOR obj IN
  (SELECT DISTINCT OBJECT_NAME
  FROM USER_OBJECTS
  WHERE(OBJECT_TYPE = 'TABLE'
  OR OBJECT_TYPE    = 'VIEW')
  AND OBJECT_NAME  <> 'schema_version'
  )
  LOOP
    BEGIN
      EXECUTE IMMEDIATE 'GRANT SELECT ON CBE_BSE_OWNER.' || sys.DBMS_ASSERT.qualified_sql_name(obj.OBJECT_NAME) || ' TO ' || sys.DBMS_ASSERT.qualified_sql_name('APS_CHECK_RO');
    EXCEPTION
    WHEN OTHERS THEN
      IF SQLCODE != -1917 THEN
        raise;
      END IF;
    END;
  END LOOP;
END;
/
