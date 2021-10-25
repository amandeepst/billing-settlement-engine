SELECT per_id_nbr   AS partyId,
       cis_division AS legalCounterparty,
       currency_cd  AS currency,
       sa_bal       AS balance
  FROM vwm_merch_acct_ledger_snapshot
  WHERE trim(sa_type_cd)   = 'WAF'
    AND       per_id_nbr   = :per_id_nbr
    AND       cis_division = substr(:cis_division, -5)
    AND       currency_cd  = :currency_cd
