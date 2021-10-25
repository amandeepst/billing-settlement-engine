package com.worldpay.pms.bse.engine.transformations.aggregation;

import static com.worldpay.pms.bse.engine.Encodings.PENDING_BILL_AGG_KEY_ENCODER;
import static com.worldpay.pms.bse.engine.transformations.aggregation.PendingBillAggregationUtils.addNullableAmounts;
import static com.worldpay.pms.bse.engine.transformations.aggregation.PendingBillAggregationUtils.aggregateByKey;
import static com.worldpay.pms.bse.engine.transformations.aggregation.PendingBillAggregationUtils.getFieldValue;
import static com.worldpay.pms.bse.engine.transformations.aggregation.PendingBillAggregationUtils.getFieldValueOfAnyIfTrue;
import static com.worldpay.pms.bse.engine.transformations.aggregation.PendingBillAggregationUtils.validateAtMostOneIsNonNull;
import static com.worldpay.pms.bse.engine.transformations.aggregation.PendingBillAggregationUtils.warnAfterThreshold;
import static com.worldpay.pms.spark.core.SparkUtils.timed;
import static com.worldpay.pms.spark.core.TransformationsUtils.instrumentReduce;

import com.worldpay.pms.bse.engine.Encodings;
import com.worldpay.pms.bse.engine.transformations.model.billprice.BillPriceRow;
import com.worldpay.pms.bse.engine.transformations.model.completebill.BillLineDetail;
import com.worldpay.pms.bse.engine.transformations.model.pendingbill.PendingBillLineCalculationRow;
import com.worldpay.pms.bse.engine.transformations.model.pendingbill.PendingBillLineRow;
import com.worldpay.pms.bse.engine.transformations.model.pendingbill.PendingBillLineServiceQuantityRow;
import com.worldpay.pms.bse.engine.transformations.model.pendingbill.PendingBillRow;
import io.vavr.collection.Stream;
import java.util.function.Function;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.spark.sql.Dataset;
import scala.Tuple2;

@Slf4j
@UtilityClass
public class PendingBillAggregation {

  private static final long DEFAULT_PARTITION_ID = -1L;
  private static final int THRESHOLD_FOR_LARGE_LINE_NUMBER_WARN = 75000;
  private static final long THRESHOLD_FOR_REDUCE_LOGGING_IN_MS = 1000;

  public static Dataset<PendingBillRow> aggregate(Dataset<PendingBillRow> pendingBills) {
    return timed("aggregate-pending-bills",
        () -> pendingBills
            .groupByKey(PendingBillRow::aggregationKey, PENDING_BILL_AGG_KEY_ENCODER)
            .reduceGroups(instrumentReduce(timed("reduce-pending-bills", THRESHOLD_FOR_REDUCE_LOGGING_IN_MS,
                () -> PendingBillAggregation::reduce)))
            .map(Tuple2::_2, Encodings.PENDING_BILL_ENCODER));
  }

  private static PendingBillRow reduce(PendingBillRow x, PendingBillRow y) {
    // Either x or y, but not both, can have non null id
    validatePendingBillsForAggregation(x, y);

    PendingBillLineRow[] pendingBillLines = aggregateByKey(x.getPendingBillLines(), y.getPendingBillLines(),
        PendingBillLineRow.class, PendingBillLineRow[]::new, PendingBillLineRow::aggregationKey, PendingBillAggregation::reduce);

    PendingBillRow nonNullRow = PendingBillAggregationUtils.getNonNullId(x, y, PendingBillRow::getBillId);

    warnLargeNumberOfLines(
        nonNullRow.getBillId(),
        getFieldValue(x, PendingBillRow::getAccountId),
        nonNullRow.getBillSubAccountId(),
        pendingBillLines
    );

    BillPriceRow[] billPrices = ArrayUtils.addAll(x.getBillPrices(), y.getBillPrices());

    return new PendingBillRow(
        nonNullRow.getBillId(),
        nonNullRow.getPartyId(),
        nonNullRow.getLegalCounterpartyId(),
        getFieldValue(x, PendingBillRow::getAccountId),
        nonNullRow.getBillSubAccountId(),
        nonNullRow.getAccountType(),
        nonNullRow.getBusinessUnit(),
        nonNullRow.getBillCycleId(),
        nonNullRow.getScheduleStart(),
        getFieldValue(x, PendingBillRow::getScheduleEnd),
        nonNullRow.getCurrency(),
        nonNullRow.getBillReference(),
        getFieldValue(x, PendingBillRow::getAdhocBillIndicator),
        getFieldValue(x, PendingBillRow::getSettlementSubLevelType),
        getFieldValue(x, PendingBillRow::getSettlementSubLevelValue),
        nonNullRow.getGranularity(),
        getFieldValue(x, PendingBillRow::getGranularityKeyValue),
        getFieldValue(x, PendingBillRow::getDebtDate),
        getFieldValue(x, PendingBillRow::getDebtMigrationType),
        getFieldValue(x, PendingBillRow::getOverpaymentIndicator),
        getFieldValue(x, PendingBillRow::getReleaseWAFIndicator),
        getFieldValue(x, PendingBillRow::getReleaseReserveIndicator),
        getFieldValue(x, PendingBillRow::getFastestPaymentRouteIndicator),
        getFieldValue(x, PendingBillRow::getCaseIdentifier),
        getFieldValue(x, PendingBillRow::getIndividualBillIndicator),
        getFieldValue(x, PendingBillRow::getManualBillNarrative),
        getFieldValueOfAnyIfTrue(x, y, PendingBillRow ::getMiscalculationFlag),
        nonNullRow.getFirstFailureOn(),
        nonNullRow.getRetryCount(),
        DEFAULT_PARTITION_ID,
        pendingBillLines,
        billPrices,
        null,
        null,
        null,
        0
    );
  }

  private static void warnLargeNumberOfLines(String billId, String accountId, String subAccountId, PendingBillLineRow[] lines) {
    int linesNo = lines.length;
    int lineDetailsNo = Stream.of(lines).map(l -> l.getBillLineDetails() == null ? 0 : l.getBillLineDetails().length).sum().intValue();

    warnAfterThreshold(linesNo, THRESHOLD_FOR_LARGE_LINE_NUMBER_WARN,
        "Reached high number of bill lines: billId={}, accountId={}, subAccountId={}, linesNo={}, lineDetailsNo={}",
        billId, accountId, subAccountId, linesNo, lineDetailsNo);
  }

  private static PendingBillLineRow reduce(PendingBillLineRow x, PendingBillLineRow y) {
    // Either x or y, but not both, can have non null id
    validatePendingBillLinesForAggregation(x, y);

    BillLineDetail[] billLineDetails = ArrayUtils.addAll(x.getBillLineDetails(), y.getBillLineDetails());

    PendingBillLineCalculationRow[] pendingBillLineCalculations = aggregateByKey(x.getPendingBillLineCalculations(),
        y.getPendingBillLineCalculations(), PendingBillLineCalculationRow.class, PendingBillLineCalculationRow[]::new,
        PendingBillLineCalculationRow::aggregationKey, PendingBillAggregation::reduce);

    PendingBillLineServiceQuantityRow[] pendingBillLineServiceQuantities = aggregateByKey(x.getPendingBillLineServiceQuantities(),
        y.getPendingBillLineServiceQuantities(), PendingBillLineServiceQuantityRow.class, PendingBillLineServiceQuantityRow[]::new,
        PendingBillLineServiceQuantityRow::aggregationKey, PendingBillAggregation::reduce);

    return new PendingBillLineRow(
        getFieldValueOfNonNullId(x, y, PendingBillLineRow::getBillLineId),
        getFieldValue(x, PendingBillLineRow::getBillLinePartyId),
        getFieldValueOfNonNullId(x, y, PendingBillLineRow::getProductClass),
        getFieldValue(x, PendingBillLineRow::getProductIdentifier),
        getFieldValue(x, PendingBillLineRow::getPricingCurrency),
        getFieldValue(x, PendingBillLineRow::getFundingCurrency),
        addNullableAmounts(x, y, PendingBillLineRow::getFundingAmount),
        getFieldValue(x, PendingBillLineRow::getTransactionCurrency),
        addNullableAmounts(x, y, PendingBillLineRow::getTransactionAmount),
        addNullableAmounts(x, y, PendingBillLineRow::getQuantity),
        getFieldValue(x, PendingBillLineRow::getPriceLineId),
        getFieldValue(x, PendingBillLineRow::getMerchantCode),
        DEFAULT_PARTITION_ID,
        billLineDetails,
        pendingBillLineCalculations,
        pendingBillLineServiceQuantities
    );
  }

  public static PendingBillLineCalculationRow reduce(PendingBillLineCalculationRow x, PendingBillLineCalculationRow y) {
    // Either x or y, but not both, can have non null id
    validatePendingBillLineCalculationsForAggregation(x, y);

    return new PendingBillLineCalculationRow(
        getFieldValueOfNonNullId(x, y, PendingBillLineCalculationRow::getBillLineCalcId),
        getFieldValue(x, PendingBillLineCalculationRow::getCalculationLineClassification),
        getFieldValue(x, PendingBillLineCalculationRow::getCalculationLineType),
        addNullableAmounts(x, y, PendingBillLineCalculationRow::getAmount),
        getFieldValue(x, PendingBillLineCalculationRow::getIncludeOnBill),
        getFieldValue(x, PendingBillLineCalculationRow::getRateType),
        getFieldValueOfNonNullId(x, y, PendingBillLineCalculationRow::getRateValue),
        DEFAULT_PARTITION_ID);
  }

  public static PendingBillLineServiceQuantityRow reduce(PendingBillLineServiceQuantityRow x, PendingBillLineServiceQuantityRow y) {
    return new PendingBillLineServiceQuantityRow(
        getFieldValue(x, PendingBillLineServiceQuantityRow::getServiceQuantityTypeCode),
        addNullableAmounts(x, y, PendingBillLineServiceQuantityRow::getServiceQuantityValue),
        DEFAULT_PARTITION_ID);
  }

  private static void validatePendingBillsForAggregation(PendingBillRow x, PendingBillRow y) {
    validateAtMostOneIsNonNull(x, y, PendingBillRow::getBillId, () -> new PendingBillAggregationException(
        String.format("Cannot merge 2 pending bills with non null ids: `%s` and `%s`", x.getBillId(), y.getBillId())));
  }

  private static void validatePendingBillLinesForAggregation(PendingBillLineRow x, PendingBillLineRow y) {
    validateAtMostOneIsNonNull(x, y, PendingBillLineRow::getBillLineId, () -> new PendingBillAggregationException(
        String.format("Cannot merge 2 pending bill lines with non null ids: `%s` and `%s`", x.getBillLineId(), y.getBillLineId())));
  }

  private static void validatePendingBillLineCalculationsForAggregation(PendingBillLineCalculationRow x, PendingBillLineCalculationRow y) {
    validateAtMostOneIsNonNull(x, y, PendingBillLineCalculationRow::getBillLineCalcId, () -> new PendingBillAggregationException(
        String.format("Cannot merge 2 pending bill line calculations with non null ids: `%s` and `%s`",
            x.getBillLineCalcId(), y.getBillLineCalcId())));
  }

  private static <R> R getFieldValueOfNonNullId(PendingBillLineRow x, PendingBillLineRow y, Function<PendingBillLineRow, R> fieldGetter) {
    return PendingBillAggregationUtils.getFieldValueOfNonNullId(x, y, PendingBillLineRow::getBillLineId, fieldGetter);
  }

  private static <R> R getFieldValueOfNonNullId(PendingBillLineCalculationRow x, PendingBillLineCalculationRow y,
      Function<PendingBillLineCalculationRow, R> fieldGetter) {
    return PendingBillAggregationUtils.getFieldValueOfNonNullId(x, y, PendingBillLineCalculationRow::getBillLineCalcId, fieldGetter);
  }
}