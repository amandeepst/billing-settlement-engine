begin
  pkg_privs.pr_grant_object_privs(pkg_opr.BILLING_AND_FUNDING);
end;
/
begin
  pkg_synonyms.pr_create_synonyms(pkg_opr.BILLING_AND_FUNDING);
end;
/