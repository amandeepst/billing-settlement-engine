package com.worldpay.pms.bse.engine.transformations.model.pendingbill;

import com.worldpay.pms.bse.domain.model.PendingBill;
import com.worldpay.pms.bse.engine.transformations.model.billprice.BillPriceRow;
import com.worldpay.pms.bse.engine.transformations.model.completebill.BillLineDetailRow;
import com.worldpay.pms.bse.engine.transformations.model.input.pending.InputPendingBillRow;
import com.worldpay.pms.bse.engine.transformations.model.pendingbill.aggregation.PendingBillAggKey;
import io.vavr.collection.Stream;
import java.sql.Date;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder(toBuilder = true)
public class PendingBillRow implements PendingBill {

  private String billId;
  private String partyId;
  private String legalCounterpartyId;
  private String accountId;
  private String billSubAccountId;
  private String accountType;
  private String businessUnit;
  private String billCycleId;
  private Date scheduleStart;
  private Date scheduleEnd;
  private String currency;
  private String billReference;
  private String adhocBillIndicator;
  private String settlementSubLevelType;
  private String settlementSubLevelValue;
  private String granularity;
  private String granularityKeyValue;
  private Date debtDate;
  private String debtMigrationType;
  private String overpaymentIndicator;
  private String releaseWAFIndicator;
  private String releaseReserveIndicator;
  private String fastestPaymentRouteIndicator;
  private String caseIdentifier;
  private String individualBillIndicator;
  private String manualBillNarrative;
  private String miscalculationFlag;

  private Date firstFailureOn;
  private short retryCount;

  private long partitionId;

  private PendingBillLineRow[] pendingBillLines;
  private BillPriceRow[] billPrices;

  private String billableItemId;
  private Date billableItemIlmDate;
  private Date billableItemFirstFailureOn;
  private int billableItemRetryCount;

  public static PendingBillRow from(InputPendingBillRow raw, PendingBillLineRow[] pendingBillLines) {
    return new PendingBillRow(
        raw.getBillId(),
        raw.getPartyId(),
        raw.getLegalCounterpartyId(),
        raw.getAccountId(),
        raw.getBillSubAccountId(),
        raw.getAccountType(),
        raw.getBusinessUnit(),
        raw.getBillCycleId(),
        raw.getScheduleStart(),
        raw.getScheduleEnd(),
        raw.getCurrency(),
        raw.getBillReference(),
        raw.getAdhocBillIndicator(),
        raw.getSettlementSubLevelType(),
        raw.getSettlementSubLevelValue(),
        raw.getGranularity(),
        raw.getGranularityKeyValue(),
        raw.getDebtDate(),
        raw.getDebtMigrationType(),
        raw.getOverpaymentIndicator(),
        raw.getReleaseWAFIndicator(),
        raw.getReleaseReserveIndicator(),
        raw.getFastestPaymentRouteIndicator(),
        raw.getCaseIdentifier(),
        raw.getIndividualBillIndicator(),
        raw.getManualBillNarrative(),
        raw.getMiscalculationFlag(),
        raw.getFirstFailureOn(),
        raw.getRetryCount(),
        raw.getPartitionId(),
        pendingBillLines,
        new BillPriceRow[]{},
        null,
        null,
        null,
        0
    );
  }

  public PendingBillAggKey aggregationKey() {
    return new PendingBillAggKey(
        accountId,
        scheduleEnd,
        adhocBillIndicator,
        settlementSubLevelType,
        settlementSubLevelValue,
        granularityKeyValue,
        debtDate,
        debtMigrationType,
        overpaymentIndicator,
        releaseWAFIndicator,
        releaseReserveIndicator,
        fastestPaymentRouteIndicator,
        caseIdentifier,
        individualBillIndicator,
        manualBillNarrative
    );
  }

  public BillLineDetailRow[] billLineDetails() {
    String billId = getBillId();
    return Stream.of(getPendingBillLines())
        .flatMap(pendingBillLine ->
            Stream.of(BillLineDetailRow.from(billId, pendingBillLine.getBillLineId(), pendingBillLine.getBillLineDetails())))
        .toJavaArray(BillLineDetailRow[]::new);
  }
}
