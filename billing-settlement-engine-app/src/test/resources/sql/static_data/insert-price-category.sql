INSERT INTO cm_price_category (
    price_ctg_id,
    descr,
    valid_from,
    valid_to
) VALUES (
    :price_ctg_id,
    :descr,
    SYSDATE,
    null
);
