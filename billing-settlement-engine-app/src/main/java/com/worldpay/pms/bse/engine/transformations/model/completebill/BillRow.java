package com.worldpay.pms.bse.engine.transformations.model.completebill;

import com.worldpay.pms.bse.domain.common.Utils;
import com.worldpay.pms.bse.domain.model.Bill;
import com.worldpay.pms.bse.domain.model.billtax.BillTax;
import com.worldpay.pms.bse.domain.model.billtax.BillTaxDetail;
import com.worldpay.pms.bse.domain.model.completebill.CompleteBill;
import com.worldpay.pms.bse.engine.transformations.model.billprice.BillPriceRow;
import com.worldpay.pms.bse.engine.transformations.model.input.correction.InputBillCorrectionRow;
import com.worldpay.pms.bse.engine.transformations.model.pendingbill.PendingBillRow;
import java.math.BigDecimal;
import java.sql.Date;
import java.util.Arrays;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.With;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder(toBuilder = true)
public class BillRow implements Bill {

  private static final short ZERO = (short) 0;

  @NonNull
  private String billId;
  @NonNull
  private String partyId;
  @NonNull
  private String billSubAccountId;
  private String tariffType;
  private String templateType;
  @NonNull
  private String legalCounterparty;
  @NonNull
  private String accountType;
  @NonNull
  private String accountId;
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

  private String merchantTaxRegistrationNumber;
  private String worldpayTaxRegistrationNumber;
  private String taxType;
  private String taxAuthority;
  private String correctionEventId;
  private String correctionPaidInvoice;
  private String correctionType;
  private String correctionReasonCd;

  @With
  private BillTax billTax;

  private BillLineRow[] billLines;
  private BillLineDetailRow[] billLineDetails;
  private BillPriceRow[] billPrices;

  private Date firstFailureOn;
  private short retryCount;

  public static BillRow from(String billId, @NonNull CompleteBill completeBill, @NonNull BillLineRow[] billLines, PendingBillRow pendingBill) {
    BillLineDetailRow[] billLineDetails = (pendingBill == null) ? new BillLineDetailRow[0] : pendingBill.billLineDetails();
    BillPriceRow[] billPrices = (pendingBill == null) ? new BillPriceRow[0] : pendingBill.getBillPrices();
    Date firstFailureOn = (pendingBill == null) ? null : pendingBill.getFirstFailureOn();
    short retryCount = (pendingBill == null) ? ZERO : pendingBill.getRetryCount();

    BillTax billTax = completeBill.getBillTax() == null ? completeBill.getBillTax() : completeBill.getBillTax().withBillId(billId);
    BigDecimal totalTaxAmount = billTax == null ? BigDecimal.ZERO
        : Arrays.stream(billTax.getBillTaxDetails()).map(BillTaxDetail::getTaxAmount).reduce(BigDecimal.ZERO, BigDecimal::add);
    String merchantTaxRegistrationNumber = billTax == null ? null : billTax.getMerchantTaxRegistrationNumber();
    String worldpayTaxRegistrationNumber = billTax == null ? null : billTax.getWorldpayTaxRegistrationNumber();
    String taxType = billTax == null ? null : billTax.getTaxType();
    String taxAuthority = billTax == null ? null : billTax.getTaxAuthority();

    return new BillRow(
        billId,
        completeBill.getPartyId(),
        completeBill.getBillSubAccountId(),
        null,
        null,
        completeBill.getLegalCounterpartyId(),
        completeBill.getAccountType(),
        completeBill.getAccountId(),
        completeBill.getBusinessUnit(),
        Utils.getDate(completeBill.getScheduleEnd()),
        completeBill.getBillCycleId(),
        Utils.getDate(completeBill.getScheduleStart()),
        Utils.getDate(completeBill.getScheduleEnd()),
        completeBill.getCurrency(),
        completeBill.getBillAmount().add(totalTaxAmount),
        completeBill.getBillReference(),
        "COMPLETED",
        completeBill.getAdhocBillIndicator(),
        completeBill.getSettlementSubLevelType(),
        completeBill.getSettlementSubLevelValue(),
        completeBill.getGranularity(),
        completeBill.getGranularityKeyValue(),
        completeBill.getReleaseWAFIndicator(),
        completeBill.getReleaseReserveIndicator(),
        completeBill.getFastestPaymentRouteIndicator(),
        completeBill.getCaseIdentifier(),
        completeBill.getIndividualBillIndicator(),
        completeBill.getManualBillNarrative(),
        completeBill.getProcessingGroup(),
        // how do we populate previous Bill Id?
        null,
        Utils.getDate(completeBill.getDebtDate()),
        completeBill.getDebtMigrationType(),
        merchantTaxRegistrationNumber,
        worldpayTaxRegistrationNumber,
        taxType,
        taxAuthority,
        null,
        null,
        null,
        null,
        billTax,
        billLines,
        billLineDetails,
        billPrices,
        firstFailureOn,
        retryCount
    );
  }

  public static BillRow from(InputBillCorrectionRow billCorrectionRow, BillLineRow[] billLineRows) {
    return new BillRow(
        billCorrectionRow.getBillId(),
        billCorrectionRow.getPartyId(),
        billCorrectionRow.getBillSubAccountId(),
        billCorrectionRow.getTariffType(),
        billCorrectionRow.getTemplateType(),
        billCorrectionRow.getLegalCounterparty(),
        billCorrectionRow.getAccountType(),
        billCorrectionRow.getAccountId(),
        billCorrectionRow.getBusinessUnit(),
        billCorrectionRow.getBillDate(),
        billCorrectionRow.getBillCycleId(),
        billCorrectionRow.getStartDate(),
        billCorrectionRow.getEndDate(),
        billCorrectionRow.getCurrencyCode(),
        billCorrectionRow.getBillAmount(),
        billCorrectionRow.getBillReference(),
        billCorrectionRow.getStatus(),
        billCorrectionRow.getAdhocBillFlag(),
        billCorrectionRow.getSettlementSubLevelType(),
        billCorrectionRow.getSettlementSubLevelValue(),
        billCorrectionRow.getGranularity(),
        billCorrectionRow.getGranularityKeyValue(),
        billCorrectionRow.getReleaseWafIndicator(),
        billCorrectionRow.getReleaseReserveIndicator(),
        billCorrectionRow.getFastestPaymentRouteIndicator(),
        billCorrectionRow.getCaseId(),
        billCorrectionRow.getIndividualBillIndicator(),
        billCorrectionRow.getManualNarrative(),
        billCorrectionRow.getProcessingGroup(),
        billCorrectionRow.getPreviousBillId(),
        billCorrectionRow.getDebtDate(),
        billCorrectionRow.getDebtMigrationType(),
        billCorrectionRow.getMerchantTaxRegistrationNumber(),
        billCorrectionRow.getWorldpayTaxRegistrationNumber(),
        billCorrectionRow.getTaxType(),
        billCorrectionRow.getTaxAuthority(),
        billCorrectionRow.getCorrectionEventId(),
        billCorrectionRow.getPaidInvoice(),
        billCorrectionRow.getType(),
        billCorrectionRow.getReasonCd(),
        null,
        billLineRows,
        new BillLineDetailRow[0],
        new BillPriceRow[0],
        null,
        ZERO
    );
  }

  public static boolean isStandardChargingBill(BillRow row){
    return "CHRG".equals(row.getAccountType()) &&
        "N".equals(row.getAdhocBillFlag()) &&
        "N".equals(row.getIndividualBillIndicator()) &&
        ("N".equals(row.getCaseId()) || row.getCaseId() == null);
  }
}
