BEGIN
   FOR cur_syn IN (SELECT synonym_name, owner
                     FROM all_synonyms
                    WHERE owner in ('CBE_BSE_APPUSER', 'CBE_BSE_OWNER'))
   LOOP
      BEGIN
         EXECUTE IMMEDIATE 'drop synonym ' || sys.DBMS_ASSERT.qualified_sql_name(cur_syn.owner || '."' || cur_syn.synonym_name || '"');
      EXCEPTION
         WHEN OTHERS
         THEN
        DBMS_OUTPUT.PUT_LINE('failed to drop synonym ' || sys.DBMS_ASSERT.qualified_sql_name(cur_syn.owner || '."' || cur_syn.synonym_name || '"'));

      END;
   END LOOP;
END;
/