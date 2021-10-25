package com.worldpay.pms.bse.domain.tax;

import com.worldpay.pms.bse.domain.model.tax.Party;
import com.worldpay.pms.bse.domain.model.tax.ProductCharacteristic;
import com.worldpay.pms.bse.domain.model.tax.TaxRate;
import com.worldpay.pms.bse.domain.model.tax.TaxRule;
import io.vavr.collection.List;
import java.math.BigDecimal;

public class TaxRelatedSamples {

  public static final String LCP_01 = "PO1100000001";
  public static final String LCP_02 = "PO1100000002";

  public static final String MERCHANT_REGION_INTRA_EU = "INTRA-EU";
  public static final String MERCHANT_REGION_NON_EC = "NON-EC";

  public static final TaxRule RULE_01_NUL_INT_EU_Y = taxRule(LCP_01, null, MERCHANT_REGION_INTRA_EU, "Y", "GBR", "GBP", "HMRC", "VAT",
      "TX_S_OOS", "N", "GB991280207");
  public static final TaxRule RULE_01_NUL_INT_EU_N = taxRule(LCP_01, null, MERCHANT_REGION_INTRA_EU, "N", "GBR", "GBP", "HMRC", "VAT",
      "TX_S_OOS", "N", "GB991280207");
  public static final TaxRule RULE_01_NUL_NON_EC_Y = taxRule(LCP_01, null, MERCHANT_REGION_NON_EC, "Y", "GBR", "GBP", "HMRC", "VAT",
      "TX_S_OOS", "N", "GB991280207");
  public static final TaxRule RULE_01_NUL_NON_EC_N = taxRule(LCP_01, null, MERCHANT_REGION_NON_EC, "N", "GBR", "GBP", "HMRC", "VAT",
      "TX_S_OOS", "N", "GB991280207");
  public static final TaxRule RULE_01_IRL_INT_EU_Y = taxRule(LCP_01, "IRL", MERCHANT_REGION_INTRA_EU, "Y", "IRL", "EUR", "ROI", "VAT",
      "TX_S_ROI", "N", "IE 9755575T");
  public static final TaxRule RULE_01_IRL_INT_EU_N = taxRule(LCP_01, "IRL", MERCHANT_REGION_INTRA_EU, "N", "IRL", "EUR", "ROI", "VAT",
      "TX_S_ROI", "Y", "IE 9755575T");
  public static final TaxRule RULE_02_NLD_INT_EU_Y = taxRule(LCP_02, "NLD", MERCHANT_REGION_INTRA_EU, "Y", "NLD", "EUR", "NL", "VAT",
      "TX_S_NL", "N", "NL 853935051B01");
  public static final TaxRule RULE_02_NLD_INT_EU_N = taxRule(LCP_02, "NLD", MERCHANT_REGION_INTRA_EU, "N", "NLD", "EUR", "NL", "VAT",
      "TX_S_NL", "N", "NL 853935051B01");

  public static final Party PARTY_ROU_INT_EU_Y = party("PO1111111111", "ROU", MERCHANT_REGION_INTRA_EU, "01", "Y");
  public static final Party PARTY_ROU_INT_EU_N = party("PO2222222222", "ROU", MERCHANT_REGION_INTRA_EU, null, "N");
  public static final Party PARTY_AUS_NON_EC_Y = party("PO3333333333", "AUS", MERCHANT_REGION_NON_EC, "02", "Y");
  public static final Party PARTY_AUS_NON_EC_N = party("PO4444444444", "AUS", MERCHANT_REGION_NON_EC, null, "N");
  public static final Party PARTY_IRL_INT_EU_Y = party("PO5555555555", "IRL", MERCHANT_REGION_INTRA_EU, "03", "Y");
  public static final Party PARTY_IRL_INT_EU_N = party("PO6666666666", "IRL", MERCHANT_REGION_INTRA_EU, null, "N");
  public static final Party PARTY_NLD_INT_EU_Y = party("PO7777777777", "NLD", MERCHANT_REGION_INTRA_EU, "04", "Y");
  public static final Party PARTY_NLD_INT_EU_N = party("PO8888888888", "NLD", MERCHANT_REGION_INTRA_EU, null, "N");
  public static final Party PARTY_BEL_INT_EU_Y = party("PO9999999999", "BEL", MERCHANT_REGION_INTRA_EU, "05", "Y");
  public static final Party PARTY_IND_NON_EC_Y = party("PO1111333333", "UK", MERCHANT_REGION_NON_EC, "06", "Y");
  public static final Party PARTY_IND_NON_EC_N = party("PO2222333333", "UK", MERCHANT_REGION_NON_EC, null, "N");


  public static final TaxRate RATE_NL_00 = taxRate("TX_S_NL", "STANDARD-S", "S", "S Standard", BigDecimal.valueOf(21));
  public static final TaxRate RATE_NL_01 = taxRate("TX_S_NL", "EXEMPT-E", "E", "E Exempt", BigDecimal.ZERO);
  public static final TaxRate RATE_KK_02 = taxRate("TX_S_KK", "EXEMPT-E", "E", "E Exempt", BigDecimal.ZERO);
  public static final TaxRate RATE_KK_03 = taxRate("TX_S_KK", "OUT OF SCOPE-O", "O", "O Out of Scope", BigDecimal.ZERO);
  public static final TaxRate RATE_SGP_04 = taxRate("TX_S_SGP", "EXEMPT-E", "E", "E Exempt", BigDecimal.ZERO);
  public static final TaxRate RATE_SGP_05 = taxRate("TX_S_SGP", "OUT OF SCOPE-O", "O", "O Out of Scope", BigDecimal.ZERO);
  public static final TaxRate RATE_NZ_06 = taxRate("TX_S_NZ", "EXEMPT-E", "E", "E Exempt", BigDecimal.ZERO);
  public static final TaxRate RATE_NZ_07 = taxRate("TX_S_NZ", "OUT OF SCOPE-O", "O", "O Out of Scope", BigDecimal.ZERO);
  public static final TaxRate RATE_ATO_08 = taxRate("TX_S_ATO", "EXEMPT-E", "E", "E Exempt", BigDecimal.ZERO);
  public static final TaxRate RATE_ATO_09 = taxRate("TX_S_ATO", "OUT OF SCOPE-O", "O", "O Out of Scope", BigDecimal.ZERO);
  public static final TaxRate RATE_HK_10 = taxRate("TX_S_HK", "OUT OF SCOPE-O", "O", "O Out of Scope", BigDecimal.ZERO);
  public static final TaxRate RATE_NL_11 = taxRate("TX_S_NL", "ZERO-Z", "Z", "Z Zero", BigDecimal.ZERO);
  public static final TaxRate RATE_NL_12 = taxRate("TX_S_NL", "OUT OF SCOPE-O", "O", "O Out of Scope", BigDecimal.ZERO);
  public static final TaxRate RATE_ATO_13 = taxRate("TX_S_ATO", "GST STD-S", "S", "GST Standard", BigDecimal.valueOf(10));
  public static final TaxRate RATE_ATO_14 = taxRate("TX_S_ATO", "GST ZERO-Z", "Z", "GST Zero", BigDecimal.ZERO);
  public static final TaxRate RATE_HK_15 = taxRate("TX_S_HK", "GST STD-S", "S", "GST Standard", BigDecimal.valueOf(5));
  public static final TaxRate RATE_HK_16 = taxRate("TX_S_HK", "GST ZERO-Z", "Z", "GST Zero", BigDecimal.ZERO);
  public static final TaxRate RATE_UK_17 = taxRate("TX_S_UK", "EXEMPT-E", "E", "E Exempt", BigDecimal.ZERO);
  public static final TaxRate RATE_UK_18 = taxRate("TX_S_UK", "OUT OF SCOPE-O", "O", "O Out of Scope", BigDecimal.ZERO);
  public static final TaxRate RATE_UK_19 = taxRate("TX_S_UK", "STANDARD-S", "S", "S Standard", BigDecimal.valueOf(20));
  public static final TaxRate RATE_UK_20 = taxRate("TX_S_UK", "ZERO-Z", "Z", "Z Zero", BigDecimal.ZERO);
  public static final TaxRate RATE_IND_21 = taxRate("TX_S_IND", "IST 2ND HIGHER-H", "H", "H Higher", BigDecimal.valueOf(1));
  public static final TaxRate RATE_IND_22 = taxRate("TX_S_IND", "IST EDUCATION-E ", "E", "E Education", BigDecimal.valueOf(2));
  public static final TaxRate RATE_IND_23 = taxRate("TX_S_IND", "IST SERVICE-S", "S", "S Service", BigDecimal.valueOf(12));
  public static final TaxRate RATE_IND_24 = taxRate("TX_S_IND", "IST ZERO-Z", "Z", "Z Zero", BigDecimal.ZERO);
  public static final TaxRate RATE_KK_25 = taxRate("TX_S_KK", "JCT STD-S", "S", "S Standard", BigDecimal.valueOf(10));
  public static final TaxRate RATE_KK_26 = taxRate("TX_S_KK", "JCT ZERO-Z", "Z", "Z Zero", BigDecimal.ZERO);
  public static final TaxRate RATE_NL_27 = taxRate("TX_S_NL", "GENERAL TARIFF-G", "G", "G General", BigDecimal.valueOf(21));
  public static final TaxRate RATE_NL_28 = taxRate("TX_S_NL", "LOW TARIFF-L", "L", "L Low Tariff", BigDecimal.valueOf(6));
  public static final TaxRate RATE_NL_29 = taxRate("TX_S_NL", "ZERO TARIFF-Z", "Z", "Z Zero Tariff", BigDecimal.ZERO);
  public static final TaxRate RATE_NZ_30 = taxRate("TX_S_NZ", "GST STD-S", "S", "GST Standard", BigDecimal.valueOf(15));
  public static final TaxRate RATE_NZ_31 = taxRate("TX_S_NZ", "GST ZERO-Z", "Z", "GST Zero", BigDecimal.ZERO);
  public static final TaxRate RATE_ROI_32 = taxRate("TX_S_ROI", "2 REDUCED RT-SR ", "R", "SR Reduced Rate", BigDecimal.valueOf(9));
  public static final TaxRate RATE_ROI_33 = taxRate("TX_S_ROI", "REDUCED RATE-R", "R", "R Reduced Rate", BigDecimal.valueOf(13.5));
  public static final TaxRate RATE_ROI_34 = taxRate("TX_S_ROI", "STANDARD-S", "S", "S Standard", BigDecimal.valueOf(23));
  public static final TaxRate RATE_ROI_35 = taxRate("TX_S_ROI", "ZERO-Z", "Z", "Z Zero", BigDecimal.ZERO);
  public static final TaxRate RATE_SGP_36 = taxRate("TX_S_SGP", "GST STD-S", "S", "GST Standard", BigDecimal.valueOf(7));
  public static final TaxRate RATE_SGP_37 = taxRate("TX_S_SGP", "GST ZERO-Z", "Z", "GST Zero", BigDecimal.ZERO);
  public static final TaxRate RATE_ROI_38 = taxRate("TX_S_ROI", "EXEMPT-E", "E", "E Exempt", BigDecimal.ZERO);
  public static final TaxRate RATE_CAN_39 = taxRate("TX_S_CAN", "OUT OF SCOPE-O", "O", "O Out of Scope", BigDecimal.ZERO);
  public static final TaxRate RATE_MY_40 = taxRate("TX_S_MY", "STANDARD-S", "S", "SST Standard ", BigDecimal.valueOf(6));
  public static final TaxRate RATE_MY_41 = taxRate("TX_S_MY", "EXEMPT-E", "E", "E Exempt", BigDecimal.ZERO);
  public static final TaxRate RATE_MY_42 = taxRate("TX_S_MY", "OUT OF SCOPE-O", "O", "O Out of Scope", BigDecimal.ZERO);

  public static final ProductCharacteristic PRMCCC06_TX_S_ROI = productCharacteristic("PRMCCC06", "TX_S_ROI", "EXEMPT-E");
  public static final ProductCharacteristic PRMCCC07_TX_S_ROI = productCharacteristic("PRMCCC07", "TX_S_ROI", "GST STD-S");

  public static final List<TaxRule> TAX_RULES = List.of(
      RULE_01_NUL_INT_EU_Y, RULE_01_NUL_INT_EU_N, RULE_01_NUL_NON_EC_Y, RULE_01_NUL_NON_EC_N, RULE_01_IRL_INT_EU_Y, RULE_01_IRL_INT_EU_N,
      RULE_02_NLD_INT_EU_Y, RULE_02_NLD_INT_EU_N
  );

  public static final List<Party> PARTIES = List.of(
      PARTY_ROU_INT_EU_Y, PARTY_ROU_INT_EU_N, PARTY_AUS_NON_EC_Y, PARTY_AUS_NON_EC_N, PARTY_IRL_INT_EU_Y, PARTY_IRL_INT_EU_N,
      PARTY_NLD_INT_EU_Y, PARTY_NLD_INT_EU_N, PARTY_BEL_INT_EU_Y, PARTY_IND_NON_EC_Y, PARTY_IND_NON_EC_N);

  public static final List<TaxRate> TAX_RATES = List.of(
      RATE_NL_00, RATE_NL_01, RATE_KK_02, RATE_KK_03, RATE_SGP_04, RATE_SGP_05, RATE_NZ_06, RATE_NZ_07, RATE_ATO_08, RATE_ATO_09,
      RATE_HK_10, RATE_NL_11, RATE_NL_12, RATE_ATO_13, RATE_ATO_14,
      RATE_HK_15, RATE_HK_16, RATE_UK_17, RATE_UK_18, RATE_UK_19, RATE_UK_20, RATE_IND_21, RATE_IND_22, RATE_IND_23, RATE_IND_24,
      RATE_KK_25, RATE_KK_26, RATE_NL_27, RATE_NL_28, RATE_NL_29,
      RATE_NZ_30, RATE_NZ_31, RATE_ROI_32, RATE_ROI_33, RATE_ROI_34, RATE_ROI_35, RATE_SGP_36, RATE_SGP_37, RATE_ROI_38, RATE_CAN_39,
      RATE_MY_40, RATE_MY_41, RATE_MY_42
  );

  public static final List<ProductCharacteristic> PRODUCT_CHARACTERISTICS = List.of(
      PRMCCC06_TX_S_ROI, PRMCCC07_TX_S_ROI
  );

  private static TaxRule taxRule(String lcp,
      String merchantCountry,
      String merchantRegion,
      String merchantTaxRegistered,
      String lcpCountry,
      String lcpCurrency,
      String taxAuthority,
      String taxType,
      String taxStatusType,
      String reverseCharge,
      String lcpTaxRegNumber) {
    return new TaxRule(lcp,
        lcpCountry,
        lcpCurrency,
        merchantCountry,
        merchantRegion,
        merchantTaxRegistered,
        taxAuthority,
        taxType,
        taxStatusType,
        reverseCharge,
        lcpTaxRegNumber);
  }

  private static Party party(String partyId,
      String countryId,
      String merchantRegion,
      String merchantTaxRegistration,
      String merchantTaxRegistered) {
    return new Party(
        partyId,
        countryId,
        merchantRegion,
        merchantTaxRegistration,
        merchantTaxRegistered
    );
  }

  private static TaxRate taxRate(String taxStatusType,
      String taxStatusValue,
      String taxStatusCode,
      String taxStatusDescription,
      BigDecimal taxRate) {
    return new TaxRate(
        taxStatusType,
        taxStatusValue,
        taxStatusCode,
        taxStatusDescription,
        taxRate);
  }

  private static ProductCharacteristic productCharacteristic(String productCode, String characteristicType, String characteristicValue) {
    return new ProductCharacteristic(productCode, characteristicType, characteristicValue);
  }
}
