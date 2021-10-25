BEGIN
    EXECUTE IMMEDIATE 'RENAME cm_inv_relation_stg TO cm_inv_relation_stg_bak';
EXCEPTION
    WHEN OTHERS THEN
        IF sqlcode != -04043 THEN
            RAISE;
        END IF;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'DROP SYNONYM ODI_WORK.cm_inv_relation_stg';
EXCEPTION
    WHEN OTHERS THEN
        IF sqlcode != -01434 THEN
            RAISE;
        END IF;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'DROP SYNONYM IF112_DBLINK_APPUSER.cm_inv_relation_stg';
EXCEPTION
    WHEN OTHERS THEN
        IF sqlcode != -01434 THEN
            RAISE;
        END IF;
END;
/
