ALTER TABLE bill ADD(
  merch_tax_reg  VARCHAR2(16),
  wp_tax_reg     VARCHAR2(16)   NOT NULL ENABLE,
  tax_type       VARCHAR2(10),
  tax_authority  VARCHAR2(10)
);