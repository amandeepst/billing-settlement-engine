package com.worldpay.pms.pba.engine.transformations.writers;

import com.worldpay.pms.pba.domain.model.Adjustment;
import com.worldpay.pms.pba.domain.model.Bill;
import com.worldpay.pms.pba.domain.model.BillAdjustment;
import com.worldpay.pms.pba.engine.model.output.BillAdjustmentAccountingRow;
import java.math.BigDecimal;
import java.sql.Date;
import java.time.LocalDate;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NonNull;
import lombok.experimental.UtilityClass;

@UtilityClass
public class TestData {

  public static final BillAdjustment BILL_ADJUSTMENT_1 = new BillAdjustment(
      new Adjustment("WAF", "waf percent", BigDecimal.TEN, "sa1"),
      new TestBill("498sl;j094';a3",
          "1",
          "PO3031245154",
          "1234",
          null,
          null,
          "00001",
          "4321",
          "FUND",
          "FUND",
          "BU",
          Date.valueOf(LocalDate.now()),
          "WPDY",
          Date.valueOf(LocalDate.now()),
          Date.valueOf(LocalDate.now()),
          "EUR",
          BigDecimal.TEN,
          "ref",
          "COMPLETED",
          "Y",
          null,
          null,
          "granularity",
          null,
          "N",
          "N",
          "N",
          null,
          "N",
          "N",
          null,
          null,
          null,
          null));

  public static final BillAdjustmentAccountingRow BILL_ADJUSTMENT_ACCT = BillAdjustmentAccountingRow.builder()
      .id("498sl;j094")
      .adjustmentType("WAF")
      .adjustmentDescription("WAF BUILD")
      .adjustmentAmount(BigDecimal.TEN)
      .subAccountId("1234")
      .subAccountType("WAF")
      .partyId("PO3031245154")
      .accountId("12345")
      .accountType("FUND")
      .currencyCode("GBP")
      .billAmount(BigDecimal.valueOf(-20L))
      .businessUnit("14524")
      .legalCounterparty("00001")
      .billDate(Date.valueOf(LocalDate.now()))
      .billId("15246")
      .build();

  @Data
  @AllArgsConstructor
  public static class TestBill implements Bill {

    @NonNull
    private String billId;
    @NonNull
    private String billNumber;
    @NonNull
    private String partyId;
    @NonNull
    private String billSubAccountId;
    private String tariffType;
    private String templateType;
    @NonNull
    private String legalCounterparty;
    @NonNull
    private String accountId;
    @NonNull
    private String accountType;
    @NonNull
    private String subAccountType;
    @NonNull
    private String businessUnit;
    @NonNull
    private Date billDate;
    @NonNull
    private String billCycleId;
    @NonNull
    private Date startDate;
    @NonNull
    private Date endDate;
    @NonNull
    private String currencyCode;
    @NonNull
    private BigDecimal billAmount;
    @NonNull
    private String billReference;
    @NonNull
    private String status;
    @NonNull
    private String adhocBillFlag;
    private String settlementSubLevelType;
    private String settlementSubLevelValue;
    private String granularity;
    private String granularityKeyValue;
    @NonNull
    private String releaseWafIndicator;
    @NonNull
    private String releaseReserveIndicator;
    @NonNull
    private String fastestPaymentRouteIndicator;
    private String caseId;
    @NonNull
    private String individualBillIndicator;
    @NonNull
    private String manualNarrative;
    private String processingGroup;
    private String previousBillId;
    private Date debtDate;
    private String debtMigrationType;
  }
}
