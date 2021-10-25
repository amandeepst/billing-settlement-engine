package com.worldpay.pms.bse.engine.transformations.model.billableitem;

import com.worldpay.pms.bse.domain.model.BillableItem;
import com.worldpay.pms.bse.engine.transformations.model.input.misc.MiscBillableItemLineRow;
import com.worldpay.pms.bse.engine.transformations.model.input.misc.MiscBillableItemRow;
import com.worldpay.pms.bse.engine.transformations.model.input.standard.StandardBillableItemLineRow;
import com.worldpay.pms.bse.engine.transformations.model.input.standard.StandardBillableItemRow;
import com.worldpay.pms.utils.Strings;
import io.vavr.collection.List;
import java.math.BigDecimal;
import java.sql.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.FieldNameConstants;

@Data
@Builder(toBuilder = true)
@AllArgsConstructor
@NoArgsConstructor
@FieldNameConstants
public class BillableItemRow implements BillableItem {

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

  private String productClass;

  private String productId;
  private Integer merchantAmountSignage;
  private String merchantCode;
  private String aggregationHash;
  private int partitionId;

  private String adhocBillFlag;
  private String billableItemType;

  private String overpaymentIndicator;
  private String releaseWAFIndicator;
  private String releaseReserveIndicator;
  private String fastestSettlementIndicator;
  private String caseIdentifier;
  private String individualPaymentIndicator;
  private String paymentNarrative;
  private Date debtDate;
  private String debtMigrationType;

  private String sourceType;
  private String sourceId;

  private Date firstFailureOn;
  private int retryCount;

  private Date ilmDate;

  private BillableItemLineRow[] billableItemLines;
  private BillableItemServiceQuantityRow billableItemServiceQuantity;

  public static BillableItemRow from(MiscBillableItemRow miscBillableItemRow, MiscBillableItemLineRow[] miscBillableItemLineRows) {
    BillableItemLineRow[] billableItemLines = Stream.of(miscBillableItemLineRows)
        .map(miscLine -> new BillableItemLineRow(
            "BASE_CHG",
            miscLine.getAmount(),
            miscLine.getLineCalculationType(),
            "M_PI",
            miscLine.getPrice(),
            miscLine.getPartitionId()))
        .toArray(BillableItemLineRow[]::new);

    Map<String, BigDecimal> serviceQuantities = new HashMap<>(1);
    serviceQuantities.put("TXN_VOL", miscBillableItemRow.getQuantity());
    BillableItemServiceQuantityRow billableItemServiceQuantity = new BillableItemServiceQuantityRow(
        null,
        serviceQuantities
    );

    String releaseWAFIndicator = miscBillableItemRow.getReleaseWAFIndicator();
    String releaseReserveIndicator = miscBillableItemRow.getReleaseReserveIndicator();
    String fastestSettlementIndicator = miscBillableItemRow.getFastestSettlementIndicator();
    String caseIdentifier = miscBillableItemRow.getCaseIdentifier();
    String individualPaymentIndicator = miscBillableItemRow.getIndividualPaymentIndicator();
    String paymentNarrative = miscBillableItemRow.getPaymentNarrative();
    Date debtDate = isMigration(miscBillableItemRow.getProductId()) ? miscBillableItemRow.getDebtDate() : null;

    int granularity = Objects.hash(
        releaseWAFIndicator,
        releaseReserveIndicator,
        fastestSettlementIndicator,
        caseIdentifier,
        individualPaymentIndicator,
        paymentNarrative,
        debtDate
    );

    return new BillableItemRow(
        miscBillableItemRow.getBillableItemId(),
        miscBillableItemRow.getSubAccountId(),
        miscBillableItemRow.getLegalCounterparty(),
        miscBillableItemRow.getAccruedDate(),
        miscBillableItemRow.getBillableItemId(),
        null,
        null,
        miscBillableItemRow.getCurrencyCode(),
        null,
        null,
        miscBillableItemRow.getCurrencyCode(),
        miscBillableItemRow.getCurrencyCode(),
        String.valueOf(granularity),
        miscBillableItemRow.getProductClass(),
        miscBillableItemRow.getProductId(),
        0,
        null,
        "-1",
        miscBillableItemRow.getPartitionId(),
        miscBillableItemRow.getAdhocBillFlag(),
        MISC_BILLABLE_ITEM,
        "N", // Temporary value, to be decided
        releaseWAFIndicator,
        releaseReserveIndicator,
        fastestSettlementIndicator,
        caseIdentifier,
        individualPaymentIndicator,
        paymentNarrative,
        debtDate,
        getDebtMigrationType(miscBillableItemRow.getProductId()),
        miscBillableItemRow.getSourceType(),
        miscBillableItemRow.getSourceId(),
        miscBillableItemRow.getFirstFailureOn(),
        miscBillableItemRow.getRetryCount(),
        miscBillableItemRow.getIlmDate(),
        billableItemLines,
        billableItemServiceQuantity);
  }

  public static BillableItemRow from(StandardBillableItemRow standardBillableItemRow,
      StandardBillableItemLineRow[] standardBillableItemLineRows, BillableItemServiceQuantityRow billableItemServiceQuantity) {
    BillableItemLineRow[] billableItemLines = Stream.of(standardBillableItemLineRows)
        .map(standardLine -> new BillableItemLineRow(
            standardLine.getDistributionId(),
            standardLine.getPreciseChargeAmount(),
            overrideCharacteristicValue(standardLine.getCharacteristicValue()),
            overrideRateType(standardLine.getDistributionId(), standardLine.getRateType()),
            standardLine.getRate(),
            standardLine.getPartitionId()))
        .toArray(BillableItemLineRow[]::new);

    return new BillableItemRow(
        standardBillableItemRow.getBillableItemId(),
        standardBillableItemRow.getSubAccountId(),
        standardBillableItemRow.getLegalCounterparty(),
        standardBillableItemRow.getAccruedDate(),
        standardBillableItemRow.getPriceAssignId(),
        standardBillableItemRow.getSettlementLevelType(),
        standardBillableItemRow.getSettlementGranularity(),
        standardBillableItemRow.getBillingCurrency(),
        standardBillableItemRow.getCurrencyFromScheme(),
        standardBillableItemRow.getFundingCurrency(),
        standardBillableItemRow.getPriceCurrency(),
        standardBillableItemRow.getTransactionCurrency(),
        standardBillableItemRow.getSettlementLevelGranularity(),
        standardBillableItemRow.getProductClass(),
        standardBillableItemRow.getChildProduct(),
        getMerchantAmountSignage(standardBillableItemRow.getMerchantAmountSignage()),
        standardBillableItemRow.getMerchantCode(),
        standardBillableItemRow.getAggregationHash(),
        standardBillableItemRow.getPartitionId(),
        "N",
        STANDARD_BILLABLE_ITEM,
        "N",
        "N",
        "N",
        "N",
        "N",
        "N",
        "N",
        null,
        null,
        null,
        null,
        standardBillableItemRow.getFirstFailureOn(),
        standardBillableItemRow.getRetryCount(),
        standardBillableItemRow.getIlmDate(),
        billableItemLines,
        billableItemServiceQuantity);
  }

  private static Integer getMerchantAmountSignage(String stringSignage) {
    return Strings.isNotNullOrEmptyOrWhitespace(stringSignage) && Integer.parseInt(stringSignage) < 0 ? -1 : 1;
  }

  private static String overrideCharacteristicValue(String characteristicValue) {

    if (characteristicValue == null) {
      return null;
    }

    switch (characteristicValue) {
      case "CS_BM1":
      case "CS_BM2":
        return "CS_BM";
      case "CSFXINC1":
      case "CSFXINC2":
        return "CSFXINC";
      default:
        return characteristicValue;
    }
  }

  private static String overrideRateType(String distributionId, String rateType) {
    return distributionId.contains("FX") ? "NA" : rateType;
  }

  private static boolean isMigration(String productId) {
    return List.of("MIGCHRG", "MIGFUND", "MIGCHBK").contains(productId);
  }

  private static String getDebtMigrationType(String productId) {
    if (isMigration(productId)) {
      return "MIG_DEBT";
    }
    if ("MIGCHRG2".equals(productId)) {
      return "MIG_DEBT_WITH_PAYMENT";
    }

    return null;
  }
}
