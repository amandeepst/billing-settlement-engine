ALTER TABLE bill_price
    ADD (
        granularity VARCHAR2(60),
        non_event_id NUMBER GENERATED ALWAYS AS IDENTITY START WITH 1 INCREMENT BY 1 MAXVALUE 999999999999999 NOCACHE NOORDER NOT NULL ENABLE
        );