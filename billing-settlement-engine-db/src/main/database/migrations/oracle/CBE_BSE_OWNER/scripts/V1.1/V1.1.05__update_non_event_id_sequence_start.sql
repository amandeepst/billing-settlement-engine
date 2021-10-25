DECLARE
    v_seq   NUMBER;
BEGIN
    SELECT nvl(MAX(non_event_id),0) + 1
    INTO v_seq
    FROM cbe_bse_owner.bill_price;
    EXECUTE IMMEDIATE 'ALTER SEQUENCE cbe_bse_owner.non_event_id_sq RESTART START WITH ' || v_seq;
END;
/
