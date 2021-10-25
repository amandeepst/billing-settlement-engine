package com.worldpay.pms.bse.domain.validation;

import com.worldpay.pms.bse.domain.model.BillableItem;
import com.worldpay.pms.bse.domain.model.BillableItemLine;
import com.worldpay.pms.bse.domain.model.BillableItemServiceQuantity;
import java.sql.Date;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class TestBillableItem implements BillableItem {

  private String billableItemId;
  private String subAccountId;
  private String legalCounterparty;
  private Date accruedDate;
  private String priceLineId;
  private String settlementLevelType;
  private String granularityKeyValue;
  private String billingCurrency;
  private String currencyFromScheme;
  private String fundingCurrency;
  private String priceCurrency;
  private String transactionCurrency;
  private String granularity;
  private String productId;
  private String aggregationHash;
  private int partitionId;
  private String adhocBillFlag;
  private String billableItemType;
  private String sourceType;
  private String sourceId;
  private String merchantCode;

  private String overpaymentIndicator;
  private String releaseWAFIndicator;
  private String releaseReserveIndicator;
  private String fastestSettlementIndicator;
  private String caseIdentifier;
  private String individualPaymentIndicator;
  private String paymentNarrative;
  private Date debtDate;
  private String debtMigrationType;
  private String productClass;
  private Date ilmDate;

  private BillableItemLine[] billableItemLines;
  private BillableItemServiceQuantity billableItemServiceQuantity;


  public static BillableItem getStandardBillableItem(BillableItemServiceQuantity billableItemServiceQuantities, BillableItemLine... billableItemLines) {
    return TestBillableItem.builder()
        .billableItemId("6pgskr77-8dj3-39f3-33h4-lg84h245lg7s")
        .billableItemType(STANDARD_BILLABLE_ITEM)
        .subAccountId("0311564536")
        .legalCounterparty("00018")
        .accruedDate(Date.valueOf("2021-01-26"))
        .priceLineId("priceLineId")
        .settlementLevelType(null)
        .granularityKeyValue(null)
        .billingCurrency("JPY")
        .currencyFromScheme("JPY")
        .fundingCurrency("JPY")
        .priceCurrency("GBP")
        .transactionCurrency("JPY")
        .granularity("2101260300586258")
        .productClass("ACQUIRED")
        .productId("FRAVI")
        .aggregationHash("-1175880790")
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
        .sourceType(null)
        .partitionId(61)
        .ilmDate(Date.valueOf("2021-01-01"))
        .billableItemLines(billableItemLines)
        .billableItemServiceQuantity(billableItemServiceQuantities)
        .build();
  }

  public static BillableItem getMiscBillableItem(BillableItemServiceQuantity svcQty, BillableItemLine... billableItemLines) {
    return TestBillableItem.builder()
        .billableItemId("5g3skr77-85f3-5vf3-6h34-lg84h24fj83f")
        .billableItemType(MISC_BILLABLE_ITEM)
        .subAccountId("0311564536")
        .legalCounterparty("00018")
        .accruedDate(Date.valueOf("2021-01-26"))
        .priceLineId("priceLineId")
        .settlementLevelType(null)
        .granularityKeyValue(null)
        .billingCurrency("JPY")
        .currencyFromScheme("JPY")
        .fundingCurrency("JPY")
        .priceCurrency("GBP")
        .transactionCurrency("JPY")
        .granularity("2101260300586258")
        .productClass("ACQUIRED")
        .productId("FRAVI")
        .aggregationHash("-1175880790")
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
        .sourceType(null)
        .partitionId(61)
        .ilmDate(Date.valueOf("2021-01-01"))
        .billableItemLines(billableItemLines)
        .billableItemServiceQuantity(svcQty)
        .build();
  }

}
