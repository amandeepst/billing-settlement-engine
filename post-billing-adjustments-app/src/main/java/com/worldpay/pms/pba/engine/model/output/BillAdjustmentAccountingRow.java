package com.worldpay.pms.pba.engine.model.output;

import com.worldpay.pms.pba.domain.model.BillAdjustment;
import java.math.BigDecimal;
import java.sql.Date;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

@Value
@Builder
public class BillAdjustmentAccountingRow {

  @NonNull
  String id;
  String adjustmentType;
  String adjustmentDescription;
  @NonNull
  BigDecimal adjustmentAmount;
  @NonNull
  String subAccountId;
  @NonNull
  String subAccountType;
  @NonNull
  String partyId;
  @NonNull
  String accountId;
  @NonNull
  String accountType;
  @NonNull
  String currencyCode;
  @NonNull
  BigDecimal billAmount;
  @NonNull
  String billId;
  @NonNull
  String legalCounterparty;
  String businessUnit;
  Date billDate;

  public static BillAdjustmentAccountingRow billRowFromAdjustment(BillAdjustment billAdjustment, String id){
    return BillAdjustmentAccountingRow.builder()
        .id(id)
        .billId(billAdjustment.getBill().getBillId())
        .adjustmentType(billAdjustment.getAdjustment().getType())
        .adjustmentDescription(billAdjustment.getAdjustment().getDescription())
        .adjustmentAmount(billAdjustment.getAdjustment().getAmount())
        .subAccountId(billAdjustment.getBill().getBillSubAccountId())
        .subAccountType(billAdjustment.getBill().getAccountType())
        .partyId(billAdjustment.getBill().getPartyId())
        .accountId(billAdjustment.getBill().getAccountId())
        .accountType(billAdjustment.getBill().getAccountType())
        .currencyCode(billAdjustment.getBill().getCurrencyCode())
        .billAmount(billAdjustment.getBill().getBillAmount())
        .businessUnit(billAdjustment.getBill().getBusinessUnit())
        .legalCounterparty(billAdjustment.getBill().getLegalCounterparty())
        .billDate(billAdjustment.getBill().getBillDate())
        .build();
  }

  public static BillAdjustmentAccountingRow wafRowFromAdjustment(BillAdjustment billAdjustment, String id){
    return BillAdjustmentAccountingRow.builder()
        .id(id)
        .billId(billAdjustment.getBill().getBillId())
        .adjustmentType(billAdjustment.getAdjustment().getType())
        .adjustmentDescription(billAdjustment.getAdjustment().getDescription())
        .adjustmentAmount(billAdjustment.getAdjustment().getAmount().negate())
        .subAccountId(billAdjustment.getAdjustment().getSubAccountId())
        .subAccountType(billAdjustment.getAdjustment().getSubAccountType())
        .partyId(billAdjustment.getBill().getPartyId())
        .accountId(billAdjustment.getBill().getAccountId())
        .accountType(billAdjustment.getBill().getAccountType())
        .currencyCode(billAdjustment.getBill().getCurrencyCode())
        .billAmount(billAdjustment.getBill().getBillAmount())
        .businessUnit(billAdjustment.getBill().getBusinessUnit())
        .legalCounterparty(billAdjustment.getBill().getLegalCounterparty())
        .billDate(billAdjustment.getBill().getBillDate())
        .build();

  }

}
