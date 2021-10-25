CREATE OR REPLACE VIEW vw_minimum_charge AS
SELECT a.price_asgn_id as price_assign_id,
        'PO11000' || f.cis_division AS lcp,
        c.per_id_nbr AS party_id,
        nvl(d.adhoc_char_val, 'WPMO') AS min_chg_period,
         'MIN_P_CHRG' AS rate_tp,
        e.value_amt AS rate,
        a.price_currency_cd as currency_cd,
        a.start_dt,
        a.end_dt,
        a.price_status_flag
FROM ci_priceasgn a
     INNER JOIN ci_party b ON (a.owner_id = b.party_uid)
     INNER JOIN ci_per_id c ON (b.party_id = c.per_id)
     LEFT OUTER JOIN ci_per_char d ON (c.per_id = d.per_id AND d.char_type_cd = 'MINCHGNO')
     INNER JOIN ci_pricecomp e ON (a.price_asgn_id = e.price_asgn_id)
     INNER JOIN ci_per f ON (c.per_id = f.per_id)
WHERE c.id_type_cd = 'EXPRTYID'
AND a.priceitem_cd = 'MINCHRGP'
AND e.value_amt <> 0;