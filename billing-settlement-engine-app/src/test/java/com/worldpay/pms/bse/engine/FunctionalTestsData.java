package com.worldpay.pms.bse.engine;

import com.google.common.collect.ImmutableMap;
import com.worldpay.pms.bse.domain.account.AccountDeterminationService.AccountDetails;
import com.worldpay.pms.bse.domain.account.BillingAccount;
import com.worldpay.pms.bse.domain.account.BillingCycle;
import com.worldpay.pms.bse.domain.model.billtax.BillLineTax;
import com.worldpay.pms.bse.domain.model.billtax.BillTax;
import com.worldpay.pms.bse.domain.model.billtax.BillTaxDetail;
import com.worldpay.pms.bse.domain.model.mincharge.MinimumCharge;
import com.worldpay.pms.bse.domain.model.tax.ProductCharacteristic;
import com.worldpay.pms.bse.engine.samples.TaxRelatedSamples;
import com.worldpay.pms.bse.engine.transformations.model.billableitem.BillableItemLineRow;
import com.worldpay.pms.bse.engine.transformations.model.billableitem.BillableItemRow;
import com.worldpay.pms.bse.engine.transformations.model.billableitem.BillableItemServiceQuantityRow;
import com.worldpay.pms.bse.engine.transformations.model.billprice.BillPriceRow;
import com.worldpay.pms.bse.engine.transformations.model.completebill.BillLineCalculationRow;
import com.worldpay.pms.bse.engine.transformations.model.completebill.BillLineDetail;
import com.worldpay.pms.bse.engine.transformations.model.completebill.BillLineRow;
import com.worldpay.pms.bse.engine.transformations.model.completebill.BillLineServiceQuantityRow;
import com.worldpay.pms.bse.engine.transformations.model.input.correction.InputBillCorrectionRow;
import com.worldpay.pms.bse.engine.transformations.model.input.mincharge.PendingMinChargeRow;
import com.worldpay.pms.bse.engine.transformations.model.input.misc.MiscBillableItemLineRow;
import com.worldpay.pms.bse.engine.transformations.model.input.misc.MiscBillableItemRow;
import com.worldpay.pms.bse.engine.transformations.model.pendingbill.PendingBillLineCalculationRow;
import com.worldpay.pms.bse.engine.transformations.model.pendingbill.PendingBillLineRow;
import com.worldpay.pms.bse.engine.transformations.model.pendingbill.PendingBillLineServiceQuantityRow;
import com.worldpay.pms.bse.engine.transformations.model.pendingbill.PendingBillRow;
import io.vavr.collection.Array;
import java.math.BigDecimal;
import java.sql.Date;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Stream;
import lombok.val;

public class FunctionalTestsData {

  static final List<String> EMPTY_PROCESSING_GROUP = Collections.singletonList("");
  static final List<String> STANDARD_BILLING = Collections.singletonList("STANDARD");
  static final List<String> STANDARD_AND_ADHOC_BILLING = Arrays.asList("STANDARD", "ADHOC");

  static final String WPDY = "WPDY";
  static final String WPMO = "WPMO";

  static final LocalDate DATE_2021_01_01 = LocalDate.parse("2021-01-01");
  static final LocalDate DATE_2021_01_26 = LocalDate.parse("2021-01-26");
  static final LocalDate DATE_2021_01_27 = LocalDate.parse("2021-01-27");
  static final LocalDate DATE_2021_01_28 = LocalDate.parse("2021-01-28");
  static final LocalDate DATE_2021_01_30 = LocalDate.parse("2021-01-30");
  static final LocalDate DATE_2021_01_31 = LocalDate.parse("2021-01-31");
  static final LocalDate DATE_2021_02_01 = LocalDate.parse("2021-02-01");
  static final LocalDate DATE_2021_02_28 = LocalDate.parse("2021-02-28");

  static final BillingCycle WPDY_20210127 = new BillingCycle(WPDY, DATE_2021_01_27, DATE_2021_01_27);
  static final BillingCycle WPDY_20210128 = new BillingCycle(WPDY, DATE_2021_01_28, DATE_2021_01_28);
  static final BillingCycle WPDY_20210131 = new BillingCycle(WPDY, DATE_2021_01_31, DATE_2021_01_31);
  static final BillingCycle WPDY_20210201 = new BillingCycle(WPDY, DATE_2021_02_01, DATE_2021_02_01);
  static final BillingCycle WPMO_202101 = new BillingCycle(WPMO, DATE_2021_01_01, DATE_2021_01_31);
  static final BillingCycle WPMO_202102 = new BillingCycle(WPMO, DATE_2021_02_01, DATE_2021_02_28);

  static final BillingAccount BILLING_ACCOUNT_1 = billingAccount("0300586258", "PO4008275082", "FUND", "0300301165", "FUND",
      "PO4008275082", "00018", "JPY", "PO1300000002", WPDY);
  static final BillingAccount BILLING_ACCOUNT_2 = billingAccount("0308502407", "PO4008275082", "FUND", "0300303770", "FUND",
      "PO4008275082", "00018", "MYR", "PO1300000002", WPDY);
  static final BillingAccount BILLING_ACCOUNT_3 = billingAccount("0308502408", "PO4000012155", "FUND", "5469962546", "FUND",
      "PO4000012155", "00001", "GBP", "PO1300000002", WPMO);
  static final BillingAccount BILLING_ACCOUNT_4 = billingAccount("9068590709", "PO3027880865", "FUND", "9068590906", "FUND",
      "PO3027880865", "00001", "GBP", "PO1300000002", WPMO);
  static final BillingAccount BILLING_ACCOUNT_5 = billingAccount("0308502410", "PO4008275082", "CHRG", "0300303771", "CHRG",
      "PO4008275082", "00018", "MYR", "PO1300000002", WPDY);
  static final BillingAccount BILLING_ACCOUNT_6 = billingAccount("0308502411", "PO4000012155", "CHRG", "5469962547", "CHRG",
      TaxRelatedSamples.LCP_02, TaxRelatedSamples.LCP_02, "GBP", "PO1300000002", WPMO);
  static final BillingAccount BILLING_ACCOUNT_7 = billingAccount("0308502412", "PO4000012155", "CHRG", "5469962548", "CHRG",
      TaxRelatedSamples.LCP_03, TaxRelatedSamples.LCP_03, "GBP", "PO1300000002", WPMO);
  static final BillingAccount BILLING_ACCOUNT_8 = billingAccount("0308502413", "PO4008275083", "CHRG", "0300303772", "CHRG",
      TaxRelatedSamples.LCP_02, TaxRelatedSamples.LCP_02, "JPY", "PO1300000002", WPDY);

  static final AccountDetails ACCOUNT_DETAILS_1 = new AccountDetails("0300586258", "", WPDY);
  static final AccountDetails ACCOUNT_DETAILS_2 = new AccountDetails("0308502407", "", WPDY);
  static final AccountDetails ACCOUNT_DETAILS_3 = new AccountDetails("0308502408", "", WPMO);
  static final AccountDetails ACCOUNT_DETAILS_4 = new AccountDetails("9068590709", "", WPMO);
  static final AccountDetails ACCOUNT_DETAILS_5 = new AccountDetails("9068590710", "", WPMO);
  static final AccountDetails ACCOUNT_DETAILS_6 = new AccountDetails("9068590711", "", WPMO);
  static final AccountDetails ACCOUNT_DETAILS_7 = new AccountDetails("9068590712", "", WPMO);

  static final MinimumCharge MINIMUM_CHARGE_1 = new MinimumCharge("9214474801", TaxRelatedSamples.LCP_02, TaxRelatedSamples.LCP_02,
      "MIN_P_CHRG", new BigDecimal("3000"), "JPY", new BillingCycle("WPMO", LocalDate.parse("2021-01-01"), LocalDate.parse("2021-01-31")));

  static final PendingMinChargeRow PENDING_MINIMUM_CHARGE_1 = new PendingMinChargeRow(TaxRelatedSamples.LCP_02, TaxRelatedSamples.LCP_02,
      TaxRelatedSamples.LCP_02,
      null, null, "MIN_CHG", BigDecimal.ZERO, null, "JPY", 0);

  static final BillableItemRow BILLABLE_ITEM_11 =
      standardBillableItem("6df0e527-54e3-4f01-89a8-daaaa245848d", "0300301165", "00018", "2021-01-27", "9214474800", null, null,
          "JPY", "JPY", "JPY", "GBP", "JPY", "2101260300586258", "ACQUIRED", "FRAVI", -1,
          "merchantCode", "-1175880790", 61, "2021-01-01", null, 0,
          billableItemServiceQuantity("FUNDCQPR", "F_B_MFX", 0.0391588, "P_B_EFX", 1, "AP_B_EFX", 1,
              "TXN_AMT", 1774, "TXN_VOL", 2),
          billableItemLine("SETT-CTL", 1774, "FND_AMTM", "MSC_PI", 1, 61)
      );
  static final BillableItemRow BILLABLE_ITEM_12 =
      standardBillableItem("3f585f37-666e-40c2-85b7-84fd01d0c7d3", "0300301165", "00018", "2021-01-27", "9211159046", null, null,
          "JPY", "JPY", "JPY", "GBP", "JPY", "2101260300586258", "ACQUIRED", "FPAMC", 1,
          "merchantCode", "868952478", 62, "2021-01-01", null, 0,
          billableItemServiceQuantity("FUNDCQPR", "F_B_MFX", 0.0391588, "P_B_EFX", 1, "AP_B_EFX", 1,
              "TXN_AMT", 328350, "TXN_VOL", 4),
          billableItemLine("SETT-CTL", -328350, "FND_AMTM", "MSC_PI", 1, 62)
      );
  static final BillableItemRow BILLABLE_ITEM_21 =
      standardBillableItem("d72d57ae-749c-48b7-865a-09bcedf3106f", "0300303770", "00018", "2021-01-27", "0301147033", null, null,
          "MYR", null, "JPY", "MYR", "JPY", "2101260308502407", "MISCELLANEOUS", "MAELAPMC", 0,
          null, "989450314", 57, "2021-01-01", null, 0,
          billableItemServiceQuantity("CHGPIBL", "F_B_MFX", 1, "P_B_EFX", 1, "AP_B_EFX", 1,
              "TXN_AMT", 197508, "TXN_VOL", 2),
          billableItemLine("BASE_CHG", 3, "PI_MBA", "MSC_PI", 1.5, 57)
      );
  static final BillableItemRow BILLABLE_ITEM_31 =
      standardBillableItem("db792565-9317-4495-92e5-70a203b379a8", "5469962546", "00001", "2021-01-27", "9216814145", null, null,
          "GBP", "GBP", "GBP", "GBP", "GBP", "2101315469962865", "PREMIUM", "PPVCCL01", 1,
          null, "-1983698219", 44, "2021-01-01", null, 0,
          billableItemServiceQuantity("CHGPPIBL", "F_M_AMT", 125320, "F_B_MFX", 1, "P_B_EFX", 1, "AP_B_EFX", 1,
              "TXN_AMT", 125320, "TXN_VOL", 1),
          billableItemLine("BASE_CHG", 0, "PI_MBA", "MSC_PI", 0, 44),
          billableItemLine("BASE_CHG", 0, "PC_MBA", "MSC_PC", 0, 44)
      );
  static final BillableItemRow BILLABLE_ITEM_32 =
      standardBillableItem("0dfca5a4-7893-4eac-81d3-d28f18730271", "5469962546", "00001", "2021-01-27", "9212202821", null, null,
          "GBP", "GBP", "GBP", "GBP", "GBP", "2101315469962865", "PREMIUM", "PPVCCA01", 1,
          null, "930882392", 32, "2021-01-01", null, 0,
          billableItemServiceQuantity("CHGPPIBL", "F_M_AMT", 125320, "F_B_MFX", 1, "P_B_EFX", 1, "AP_B_EFX", 1,
              "TXN_AMT", 125320, "TXN_VOL", 1),
          billableItemLine("BASE_CHG", 0, "PI_MBA", "MSC_PI", 0, 32),
          billableItemLine("BASE_CHG", 0, "PC_MBA", "MSC_PC", 0, 32)
      );

  static final BillableItemRow BILLABLE_ITEM_33 =
      standardBillableItem("5dacf5a3-1823-4ebc-81c3-d28f18530135", "5469962546", "00001", "2021-01-27", "9214102623", null, null,
          "GBP", "GBP", "GBP", "GBP", "GBP", "2101315469962865", "PREMIUM", "PPVCCL02", 1,
          null, "-831842793", 14, "2021-01-01", null, 0,
          billableItemServiceQuantity("CHGPPIBL", "F_M_AMT", 125320, "F_B_MFX", 1, "P_B_EFX", 1, "AP_B_EFX", 1,
              "TXN_AMT", 125320, "TXN_VOL", 1),
          billableItemLine("BASE_CHG", 0, "PI_MBA", "MSC_PI", 0, 14),
          billableItemLine("BASE_CHG", 0, "PC_MBA", "MSC_PC", 0, 14)
      );

  static final BillableItemRow BILLABLE_ITEM_34 =
      standardBillableItem("5dacf5a3-1823-4ebc-81c3-d28f18530136", "5469962546", "00001", "2021-01-27", "9214102623", null, null,
          "GBP", "GBP", "GBP", "GBP", "GBP", "2101315469962865", "PREMIUM", "PPVCCL02", 1,
          null, "-831842793", 14, "2021-01-01", null, 0,
          billableItemServiceQuantity("CHGPPIBL", "F_M_AMT", 125320, "F_B_MFX", 1, "P_B_EFX", 1, "AP_B_EFX", 1,
              "TXN_AMT", 125320, "TXN_VOL", 1),
          billableItemLine("BASE_CHG", 10, "PI_MBA", "MSC_PI", 0, 14),
          billableItemLine("BASE_CHG", 20, "PI_MBA", "MSC_PI", 0, 14)
      );

  static final BillableItemRow BILLABLE_ITEM_40 =
      miscBillableItem("U0drXCouR6y-", "9068590906", "PO1100000001", "2021-01-27", "Y", "GBP", "PROCESSED", "FRPAX", 1,
          "N", "N", "N", null, "Y", "N", null, null, null, "PI_MBA", 125, 125,
          "2021-02-09", null, (short) 0);
  static final BillableItemRow BILLABLE_ITEM_41 =
      miscBillableItem("U0drXCouR5y-", "9068590906", "PO1100000001", "2021-01-27", "Y", "GBP", "PROCESSED", "FRPAX", 1,
          "N", "N", "N", null, "N", "N", null, null, null, "PI_MBA", 125, 125,
          "2021-02-09", null, (short) 0);
  static final BillableItemRow BILLABLE_ITEM_42 =
      miscBillableItem("U2_vx*nsOw90", "9068590906", "PO1100000001", "2021-01-27", "Y", "GBP", "ACQUIRED", "FRJRPAX", 1,
          "N", "N", "N", null, "N", "N", null, null, null, "PI_MBA", 125, 125,
          "2021-02-09", null, (short) 0);
  static final BillableItemRow BILLABLE_ITEM_43 =
      miscBillableItem("U-qy~SYIyWn2", "9068590906", "PO1100000001", "2021-01-27", "N", "GBP", "PROCESSED", "FRPAX", 1,
          "N", "N", "N", null, "N", "N", null, null, null, "PI_MBA", 125, 125,
          "2021-02-09", null, (short) 0);

  static final BillableItemRow BILLABLE_ITEM_52_FAILED =
      standardBillableItem("0dfca5a4-7893-4eac-81d3-d28f1873-X52", "5469962546", "00001", "2021-01-27", "9212202821", null, null,
          "GBP", "GBP", "GBP", "GBP", "GBP", "2101315469962865", "PREMIUM", "PPVCCA01", 1,
          null, "930882392", 32, "2021-01-01", Date.valueOf("2021-01-11"), 22,
          billableItemServiceQuantity("CHGPPIBL", "F_M_AMT", 125320, "F_B_MFX", 1, "P_B_EFX", 1, "AP_B_EFX", 1,
              "TXN_AMT", 125320),
          billableItemLine("BASE_CHG", 0, "PI_MBA", "MSC_PI", 0, 32),
          billableItemLine("BASE_CHG", 0, "PC_MBA", "MSC_PC", 0, 32)
      );
  static final BillableItemRow BILLABLE_ITEM_53_FAILED =
      miscBillableItem("U-qy~SYIyWn-X53", "9068590906", "", "2021-01-27", "N", "GBP", "PROCESSED", "FRPAX", 1,
          "N", "N", "N", null, "N", "N", null, null, null, "PI_MBA", 125, 125,
          "2021-02-09", null, (short) 0);

  static final BillableItemRow BILLABLE_ITEM_52_FIXED =
      standardBillableItem("0dfca5a4-7893-4eac-81d3-d28f1873-X52", "5469962546", "00001", "2021-01-27", "9212202821", null, null,
          "GBP", "GBP", "GBP", "GBP", "GBP", "2101315469962865", "PREMIUM", "PPVCCA01", 1,
          null, "930882392", 32, "2021-01-01", Date.valueOf("2021-01-27"), 23,
          billableItemServiceQuantity("CHGPPIBL", "F_M_AMT", 125320, "F_B_MFX", 1, "P_B_EFX", 1, "AP_B_EFX", 1,
              "TXN_AMT", 125320, "TXN_VOL", 1),
          billableItemLine("BASE_CHG", 0, "PI_MBA", "MSC_PI", 0, 32),
          billableItemLine("BASE_CHG", 0, "PC_MBA", "MSC_PC", 0, 32)
      );
  static final BillableItemRow BILLABLE_ITEM_53_FIXED =
      miscBillableItem("U-qy~SYIyWn-X53", "9068590906", "PO1100000001", "2021-01-27", "N", "GBP", "PROCESSED", "FRPAX", 1,
          "N", "N", "N", null, "N", "N", null, "REC-CHG", "1423535", "PI_MBA", 125, 125,
          "2021-02-09", Date.valueOf("2021-01-27"), (short) 7);

  static final BillableItemRow BILLABLE_ITEM_CHRG_11 =
      standardBillableItem("6df0e527-54e3-4f01-89a8-daaaa2458452", "0300303771", "00018", "2021-01-27", "9214474800", null, null,
          "JPY", "JPY", "JPY", "GBP", "JPY", "2101260300586258", "ACQUIRED", "FRAVI", -1,
          null, "-1175880790", 61, "2021-01-01", null, 0,
          billableItemServiceQuantity("FUNDCQPR", "F_B_MFX", 0.0391588, "P_B_EFX", 1, "AP_B_EFX", 1,
              "TXN_AMT", 1774, "TXN_VOL", 2),
          billableItemLine("SETT-CTL", 1000, "CHRG", "MSC_PI", 1, 61),
          billableItemLine("SETT-CTL", 2000, "CHRG", "MSC_PI", 1, 61)
      );

  static final BillableItemRow BILLABLE_ITEM_CHRG_21 =
      standardBillableItem("6df0e527-54e3-4f01-89a8-daaaa2458452", "0300303772", "00018", "2021-01-27", "9214474801", null, null,
          "JPY", "JPY", "JPY", "GBP", "JPY", "2101260300586258", "ACQUIRED", "FRAVI", -1,
          null, "-1175880790", 61, "2021-01-01", null, 0,
          billableItemServiceQuantity("FUNDCQPR", "F_B_MFX", 10.88, "P_B_EFX", 1, "AP_B_EFX", 1,
              "TXN_AMT", 1774, "TXN_VOL", 2),
          billableItemLine("SETT-CTL", 100, "CHRG", "MSC_PI", 1, 61),
          billableItemLine("SETT-CTL", 200, "CHRG", "MSC_PI", 1, 61)
      );

  static final BillableItemRow BILLABLE_ITEM_CHRG_22 =
      standardBillableItem("6df0e527-54e3-4f01-89a8-daaaa2458452", "0300303772", "00018", "2021-01-27", "9214474801", null, null,
          "JPY", "JPY", "JPY", "GBP", "JPY", "2101260300586258", "ACQUIRED", "FRAVI", -1,
          null, "-1175880790", 61, "2021-01-01", null, 0,
          billableItemServiceQuantity("FUNDCQPR", "F_B_MFX", 10.88, "P_B_EFX", 1, "AP_B_EFX", 1,
              "TXN_AMT", 1774, "TXN_VOL", 2),
          billableItemLine("BASE_CHG", 101, "PI_MBA", "MSC_PI", 1, 61),
          billableItemLine("BASE_CHG", 201, "PC_MBA", "MSC_PI", 1, 61)
      );

  static final BillableItemRow BILLABLE_ITEM_CHRG_12 = miscBillableItem("U-qy~SYIyWn2", "5469962547", "PO1100000001", "2021-01-27", "Y",
      "ROU", "PROCESSED", "FRAVI", 1, "N", "N", "N", null, "N", "N", null, null, null, "PI_MBA", 125, 125, "2021-02-09", null, (short) 0);

  static final BillLineServiceQuantityRow BILL_LINE_SERVICE_QUANTITY_ROW_1 = new BillLineServiceQuantityRow("TXN_VOL",
      BigDecimal.valueOf(60),
      null);

  static final BillLineCalculationRow BILL_LINE_CALCULATION_ROW_1 = new BillLineCalculationRow("billCalcId", "calcLnClass",
      "calcLnType", null, BigDecimal.TEN, "Y", "TXN_AMT", BigDecimal.ONE, "EXE", null,
      "taxDescription");

  static final BillLineRow BILL_LINE_ROW_1 = new BillLineRow("billLineId", "billLineParty", "PREMIUM", "BVI",
      "productDescription", "EUR", "EUR", BigDecimal.TEN, "EUR", BigDecimal.TEN, BigDecimal.ONE,
      "priceLineId", null, BigDecimal.TEN, "EXE", null, new BillLineCalculationRow[]{BILL_LINE_CALCULATION_ROW_1},
      new BillLineServiceQuantityRow[]{BILL_LINE_SERVICE_QUANTITY_ROW_1});

  static final InputBillCorrectionRow INPUT_BILL_CORRECTION_ROW_1 = InputBillCorrectionRow.builder()
      .correctionEventId("12345678901").billId("bill_id_1").partyId("partyId1").billSubAccountId("acct_12").legalCounterparty("P0001")
      .accountType("CHRG").accountId("acct_1").businessUnit("BU01").billDate(Date.valueOf(LocalDate.now())).billCycleId("MONTHLY")
      .startDate(Date.valueOf("2021-01-01")).endDate(Date.valueOf("2021-01-31")).currencyCode("EUR").billAmount(BigDecimal.valueOf(16.3))
      .billReference("123456").status("COMPLETE").adhocBillFlag("N").releaseWafIndicator("N").releaseReserveIndicator("N")
      .fastestPaymentRouteIndicator("N").individualBillIndicator("N").manualNarrative("N")
      .paidInvoice("N").type("CANCEL").reasonCd("DPLC")
      .build();

  static final BillLineCalculationRow BILL_LINE_CALCULATION_ROW_2 = new BillLineCalculationRow("billCalcId2", "calcLnClass",
      "calcLnType", null, BigDecimal.TEN, "Y", "TXN_AMT", BigDecimal.ONE, "EXE", null,
      "taxDescription");

  static final BillLineCalculationRow BILL_LINE_CALCULATION_ROW_3 = new BillLineCalculationRow("billCalcId3", "calcLnClass",
      "calcLnType", null, BigDecimal.TEN, "Y", "TXN_AMT", BigDecimal.ONE, "EXE", null,
      "taxDescription");

  static final BillLineRow BILL_LINE_ROW_2 = new BillLineRow("billLineId2", "billLineParty", "PREMIUM", "BVI",
      "productDescription", "EUR", "EUR", BigDecimal.valueOf(20), "EUR", BigDecimal.valueOf(20), BigDecimal.ONE,
      "priceLineId", null, BigDecimal.valueOf(20), "EXE", null, new BillLineCalculationRow[]{BILL_LINE_CALCULATION_ROW_2, BILL_LINE_CALCULATION_ROW_3},
      new BillLineServiceQuantityRow[]{BILL_LINE_SERVICE_QUANTITY_ROW_1});

  static final InputBillCorrectionRow INPUT_BILL_CORRECTION_ROW_2 = InputBillCorrectionRow.builder()
      .correctionEventId("12345678901").billId("bill_id_2").partyId("partyId1").billSubAccountId("acct_12").legalCounterparty("P0001")
      .accountType("CHRG").accountId("acct_1").businessUnit("BU01").billDate(Date.valueOf(LocalDate.now())).billCycleId("MONTHLY")
      .startDate(Date.valueOf("2021-01-01")).endDate(Date.valueOf("2021-01-31")).currencyCode("EUR").billAmount(BigDecimal.valueOf(20))
      .billReference("123456").status("COMPLETE").adhocBillFlag("N").releaseWafIndicator("N").releaseReserveIndicator("N")
      .fastestPaymentRouteIndicator("N").individualBillIndicator("N").manualNarrative("N")
      .paidInvoice("N").type("CANCEL").reasonCd("DPLC")
      .build();

  static final BillTax BILL_TAX_1 = new BillTax("tax_id_1", "bill_id_1", "GB991280207", "wp_reg_1", "VAT", "HMRC", "N",
      new HashMap<String, BillLineTax>() {{
        put("bill_line_id_1", new BillLineTax("EXE", BigDecimal.ZERO, "E Exempt", BigDecimal.TEN, BigDecimal.ZERO));
        put("bill_line_id_2", new BillLineTax("S", BigDecimal.valueOf(21), "STANDARD-S", BigDecimal.TEN, BigDecimal.valueOf(2.1)));
        put("bill_line_id_3", new BillLineTax("S", BigDecimal.valueOf(21), "STANDARD-S", BigDecimal.valueOf(20), BigDecimal.valueOf(4.2)));
      }},
      new BillTaxDetail[]{
          new BillTaxDetail("tax_id_1", "EXE", BigDecimal.ZERO, "E Exempt", BigDecimal.TEN, BigDecimal.ZERO),
          new BillTaxDetail("tax_id_1", "S", BigDecimal.valueOf(21), "STANDARD-S", BigDecimal.valueOf(30), BigDecimal.valueOf(6.3))
      });

  private static BillableItemRow miscBillableItem(String billableItemId,
      String subAccountId,
      String legalCounterparty,
      String accruedDate,
      String adhocBillFlag,
      String currencyCode,
      String productClass,
      String productId,
      double quantity,
      String releaseWAFIndicator,
      String releaseReserveIndicator,
      String fastestSettlementIndicator,
      String caseIdentifier,
      String individualPaymentIndicator,
      String paymentNarrative,
      String debtDate,
      String sourceType,
      String sourceId,
      String lineCalculationType,
      double amount,
      double price,
      String ilmDate,
      Date firstFailureOn,
      short retryCount) {
    return BillableItemRow.from(
        MiscBillableItemRow.builder()
            .billableItemId(billableItemId)
            .subAccountId(subAccountId)
            .legalCounterparty(legalCounterparty)
            .accruedDate(Date.valueOf(accruedDate))
            .adhocBillFlag(adhocBillFlag)
            .currencyCode(currencyCode)
            .productClass(productClass)
            .productId(productId)
            .quantity(BigDecimal.valueOf(quantity))
            .releaseWAFIndicator(releaseWAFIndicator)
            .releaseReserveIndicator(releaseReserveIndicator)
            .fastestSettlementIndicator(fastestSettlementIndicator)
            .caseIdentifier(caseIdentifier)
            .individualPaymentIndicator(individualPaymentIndicator)
            .paymentNarrative(paymentNarrative)
            .debtDate(debtDate == null ? null : Date.valueOf(debtDate))
            .sourceType(sourceType)
            .sourceId(sourceId)
            .ilmDate(Date.valueOf(ilmDate))
            .partitionId(-1)
            .firstFailureOn(firstFailureOn)
            .retryCount(retryCount)
            .build(),
        io.vavr.collection.List.of(MiscBillableItemLineRow.builder()
            .billableItemId(billableItemId)
            .lineCalculationType(lineCalculationType)
            .amount(BigDecimal.valueOf(amount))
            .price(BigDecimal.valueOf(price))
            .partitionId(-1)
            .build()).toJavaArray(MiscBillableItemLineRow[]::new)
    );
  }

  private static BillableItemRow standardBillableItem(String billableItemId,
      String subAccountId,
      String legalCounterparty,
      String accruedDate,
      String priceLineId,
      String settlementLevelType,
      String granularityKeyValue,
      String billingCurrency,
      String currencyFromScheme,
      String fundingCurrency,
      String priceCurrency,
      String transactionCurrency,
      String granularity,
      String productClass,
      String productId,
      int merchantAmountSignage,
      String merchantCode,
      String aggregationHash,
      int partitionId,
      String ilmDate,
      Date firstFailureOn,
      int retryCount,
      BillableItemServiceQuantityRow billableItemServiceQuantity,
      BillableItemLineRow... billableItemLines) {
    return BillableItemRow.builder()
        .billableItemId(billableItemId)
        .subAccountId(subAccountId)
        .legalCounterparty(legalCounterparty)
        .accruedDate(Date.valueOf(accruedDate))
        .priceLineId(priceLineId)
        .settlementLevelType(settlementLevelType)
        .granularityKeyValue(granularityKeyValue)
        .billingCurrency(billingCurrency)
        .currencyFromScheme(currencyFromScheme)
        .fundingCurrency(fundingCurrency)
        .priceCurrency(priceCurrency)
        .transactionCurrency(transactionCurrency)
        .granularity(granularity)
        .productClass(productClass)
        .productId(productId)
        .merchantAmountSignage(merchantAmountSignage)
        .merchantCode(merchantCode)
        .aggregationHash(aggregationHash)
        .adhocBillFlag("N")
        .overpaymentIndicator("N")
        .releaseWAFIndicator("N")
        .releaseReserveIndicator("N")
        .fastestSettlementIndicator("N")
        .caseIdentifier("N")
        .individualPaymentIndicator("N")
        .paymentNarrative("N")
        .debtDate(null)
        .debtMigrationType(null)
        .billableItemType("STANDARD_BILLABLE_ITEM")
        .partitionId(partitionId)
        .ilmDate(Date.valueOf(ilmDate))
        .billableItemLines(billableItemLines)
        .billableItemServiceQuantity(billableItemServiceQuantity)
        .firstFailureOn(firstFailureOn)
        .retryCount(retryCount)
        .build();
  }

  private static BillableItemLineRow billableItemLine(String calculationLineClassification,
      double amount,
      String calculationLineType,
      String rateType,
      double rateValue,
      int partitionId) {
    return BillableItemLineRow.builder()
        .calculationLineClassification(calculationLineClassification)
        .amount(BigDecimal.valueOf(amount))
        .calculationLineType(calculationLineType)
        .rateType(rateType)
        .rateValue(BigDecimal.valueOf(rateValue))
        .partitionId(partitionId)
        .build();
  }

  private static BillableItemServiceQuantityRow billableItemServiceQuantity(String rateSchedule,
      String quantityKey1, double quantityValue1,
      String quantityKey2, double quantityValue2,
      String quantityKey3, double quantityValue3,
      String quantityKey4, double quantityValue4,
      String quantityKey5, double quantityValue5,
      String quantityKey6, double quantityValue6) {
    return BillableItemServiceQuantityRow.builder()
        .rateSchedule(rateSchedule)
        .serviceQuantities(ImmutableMap.<String, BigDecimal>builder()
            .put(quantityKey1, BigDecimal.valueOf(quantityValue1))
            .put(quantityKey2, BigDecimal.valueOf(quantityValue2))
            .put(quantityKey3, BigDecimal.valueOf(quantityValue3))
            .put(quantityKey4, BigDecimal.valueOf(quantityValue4))
            .put(quantityKey5, BigDecimal.valueOf(quantityValue5))
            .put(quantityKey6, BigDecimal.valueOf(quantityValue6))
            .build())
        .build();
  }

  private static BillableItemServiceQuantityRow billableItemServiceQuantity(String rateSchedule,
      String quantityKey1, double quantityValue1,
      String quantityKey2, double quantityValue2,
      String quantityKey3, double quantityValue3,
      String quantityKey4, double quantityValue4,
      String quantityKey5, double quantityValue5) {
    return BillableItemServiceQuantityRow.builder()
        .rateSchedule(rateSchedule)
        .serviceQuantities(ImmutableMap.<String, BigDecimal>builder()
            .put(quantityKey1, BigDecimal.valueOf(quantityValue1))
            .put(quantityKey2, BigDecimal.valueOf(quantityValue2))
            .put(quantityKey3, BigDecimal.valueOf(quantityValue3))
            .put(quantityKey4, BigDecimal.valueOf(quantityValue4))
            .put(quantityKey5, BigDecimal.valueOf(quantityValue5))
            .build())
        .build();
  }

  private static BillingAccount billingAccount(String accountId,
      String partyId,
      String accountType,
      String subAccountId,
      String subAccountType,
      String parentPartyId,
      String legalCounterparty,
      String currency,
      String businessUnit,
      String billCycleCode) {
    return BillingAccount.builder()
        .accountId(accountId)
        .childPartyId(partyId)
        .accountType(accountType)
        .subAccountId(subAccountId)
        .subAccountType(subAccountType)
        .partyId(parentPartyId)
        .legalCounterparty(legalCounterparty)
        .currency(currency)
        .businessUnit(businessUnit)
        .billingCycle(new BillingCycle(billCycleCode, WPDY_20210127.getStartDate(), WPDY_20210127.getEndDate()))
        .processingGroup("")
        .build();
  }

  static BillingAccount changeBillCycleCode(BillingAccount billingAccount, String billCycleCode) {
    return billingAccount.toBuilder()
        .billingCycle(new BillingCycle(billCycleCode, null, null))
        .build();
  }

  static AccountDetails changeBillCycleCode(AccountDetails accountDetails, String billCycleCode) {
    return accountDetails.toBuilder()
        .billingCycleCode(billCycleCode)
        .build();
  }

  static BillableItemRow changeBillableItemAccruedDate(BillableItemRow billableItem, LocalDate accruedDate) {
    return billableItem.toBuilder()
        .accruedDate(Date.valueOf(accruedDate))
        .build();
  }

  static PendingBillRow[] removeBillLineDetails(PendingBillRow[] pendingBills) {
    return Stream.of(pendingBills)
        .map(pendingBill -> pendingBill.toBuilder()
            .pendingBillLines(
                Stream.of(pendingBill.getPendingBillLines())
                    .map(pendingBillLine -> pendingBillLine.toBuilder()
                        .billLineDetails(new BillLineDetail[0])
                        .build()).toArray(PendingBillLineRow[]::new))
            .build()).toArray(PendingBillRow[]::new);
  }

  static PendingBillRow getPendingBillFrom(BillableItemRow billableItem, BillingAccount billingAccount) {
    val pendingBillLineCalculations = Arrays.stream(billableItem.getBillableItemLines())
        .map(billableItemLineRow -> new PendingBillLineCalculationRow(
            "olxM5kpyHzL5hN0olxM5kpyHzL5hN0olxM5k",
            billableItemLineRow.getCalculationLineClassification(),
            billableItemLineRow.getCalculationLineType(),
            billableItemLineRow.getAmount(),
            "Y",
            billableItemLineRow.getRateType(),
            billableItemLineRow.getRateValue(),
            billableItemLineRow.getPartitionId()
        )).toArray(PendingBillLineCalculationRow[]::new);

    val billableItemServiceQuantities = billableItem.getBillableItemServiceQuantity().getServiceQuantities();
    val pendingBillLineServiceQuantities = billableItemServiceQuantities.entrySet()
        .stream().map(entry -> new PendingBillLineServiceQuantityRow(entry.getKey(), entry.getValue(), -1L))
        .toArray(PendingBillLineServiceQuantityRow[]::new);
    val billPrices = Array.of(billableItem.getBillableItemLines()).map(
        billableItemLine ->
            new BillPriceRow(
                billingAccount.getPartyId(),
                billingAccount.getAccountType(),
                billableItem.getProductId(),
                billableItemLine.getCalculationLineType(),
                billableItemLine.getAmount(),
                billableItem.getBillingCurrency(),
                "test_bill_reference",
                "Y",
                billableItem.getSourceType(),
                billableItem.getSourceId(),
                null,
                billableItem.getBillableItemId(),
                null,
                billableItem.getAccruedDate(),
                billableItem.getGranularity()
            )).toJavaArray(BillPriceRow[]::new);

    val pendingBillLineRow = new PendingBillLineRow(
        "nlxM5kpyHzL5hN0nlxM5kpyHzL5hN0nlxM5k",
        billingAccount.getChildPartyId(),
        billableItem.getProductClass(),
        billableItem.getProductId(),
        billableItem.getPriceCurrency(),
        billableItem.getFundingCurrency(),
        billableItemServiceQuantities.get("F_M_AMT"),
        billableItem.getTransactionCurrency(),
        billableItemServiceQuantities.get("TXN_AMT"),
        billableItemServiceQuantities.get("TXN_VOL"),
        billableItem.getPriceLineId(),
        billableItem.getMerchantCode(),
        billableItem.getPartitionId(),
        BillLineDetail.array(billableItem.getBillableItemId(), billableItem.getAggregationHash()),
        pendingBillLineCalculations,
        pendingBillLineServiceQuantities
    );

    String settlementSubLevelType = null;
    String settlementSubLevelValue = null;
    if (billableItem.getSettlementLevelType() != null) {
      String[] settlementLevel = billableItem.getSettlementLevelType().split("\\|");

      if (settlementLevel.length == 2) {
        settlementSubLevelType = settlementLevel[0].trim();
        settlementSubLevelValue = settlementLevel[1].trim();
      }
    }

    return new PendingBillRow(
        "mlxM5kpyHzL5hN0",
        billingAccount.getPartyId(),
        billingAccount.getLegalCounterparty(),
        billingAccount.getAccountId(),
        billableItem.getSubAccountId(),
        billingAccount.getAccountType(),
        billingAccount.getBusinessUnit(),
        billingAccount.getBillingCycle().getCode(),
        Date.valueOf(billingAccount.getBillingCycle().getStartDate()),
        Date.valueOf(billingAccount.getBillingCycle().getEndDate()),
        billingAccount.getCurrency(),
        "test_bill_reference",
        billableItem.getAdhocBillFlag(),
        settlementSubLevelType,
        settlementSubLevelValue,
        billableItem.getGranularity(),
        billableItem.getGranularityKeyValue(),
        billableItem.getDebtDate(),
        billableItem.getDebtMigrationType(),
        billableItem.getOverpaymentIndicator(),
        billableItem.getReleaseWAFIndicator(),
        billableItem.getReleaseReserveIndicator(),
        billableItem.getFastestSettlementIndicator(),
        billableItem.getCaseIdentifier(),
        billableItem.getIndividualPaymentIndicator(),
        billableItem.getPaymentNarrative(),
        "N",
        null,
        (short) 0,
        billableItem.getPartitionId(),
        new PendingBillLineRow[]{pendingBillLineRow},
        billPrices,
        billableItem.getBillableItemId(),
        billableItem.getIlmDate(),
        billableItem.getFirstFailureOn(),
        billableItem.getRetryCount()
    );
  }

  private static ProductCharacteristic productCharacteristic(String productCode, String characteristicType, String characteristicValue) {
    return new ProductCharacteristic(productCode, characteristicType, characteristicValue);
  }
}
