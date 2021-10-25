package com.worldpay.pms.bse.engine.transformations.model.completebill;

import com.worldpay.pms.bse.domain.common.Utils;
import com.worldpay.pms.bse.domain.model.BillLine;
import com.worldpay.pms.bse.domain.model.completebill.CompleteBillLine;
import com.worldpay.pms.bse.engine.transformations.model.input.correction.InputBillLineCorrectionRow;
import java.math.BigDecimal;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder(toBuilder = true)
public class BillLineRow implements BillLine {

  @NonNull
  private String billLineId;
  @NonNull
  private String billLineParty;
  private String productClass;
  @NonNull
  private String productId;
  @NonNull
  private String productDescription;
  private String pricingCurrency;
  private String fundingCurrency;
  private BigDecimal fundingAmount;
  private String transactionCurrency;
  private BigDecimal transactionAmount;
  @NonNull
  private BigDecimal quantity;
  private String priceLineId;
  private String merchantCode;
  @NonNull
  private BigDecimal totalAmount;
  private String taxStatus;
  private String previousBillLine;

  private BillLineCalculationRow[] billLineCalculations;
  private BillLineServiceQuantityRow[] billLineServiceQuantities;

  public static BillLineRow from(CompleteBillLine completeBillLine, BillLineCalculationRow[] billLineCalculations,
      BillLineServiceQuantityRow[] billLineServiceQuantities, String productDescription, String billLineTaxStatus) {

    return new BillLineRow(
        Utils.getOrDefault(completeBillLine.getBillLineId(), Utils::generateId),
        completeBillLine.getBillLinePartyId(),
        completeBillLine.getProductClass(),
        completeBillLine.getProductIdentifier(),
        productDescription,
        completeBillLine.getPricingCurrency(),
        completeBillLine.getFundingCurrency(),
        completeBillLine.getFundingAmount(),
        completeBillLine.getTransactionCurrency(),
        completeBillLine.getTransactionAmount(),
        completeBillLine.getQuantity(),
        completeBillLine.getPriceLineId(),
        completeBillLine.getMerchantCode(),
        completeBillLine.getTotalAmount(),
        billLineTaxStatus,
        null,
        billLineCalculations,
        billLineServiceQuantities
    );
  }

  public static BillLineRow from(InputBillLineCorrectionRow billLineCorrection, BillLineCalculationRow[] billLineCalculations,
      BillLineServiceQuantityRow[] billLineServiceQuantities) {
    return new BillLineRow(
        billLineCorrection.getBillLineId(),
        billLineCorrection.getBillLineParty(),
        billLineCorrection.getProductClass(),
        billLineCorrection.getProductId(),
        billLineCorrection.getProductDescription(),
        billLineCorrection.getPricingCurrency(),
        billLineCorrection.getFundingCurrency(),
        billLineCorrection.getFundingAmount(),
        billLineCorrection.getTransactionCurrency(),
        billLineCorrection.getTransactionAmount(),
        billLineCorrection.getQuantity(),
        billLineCorrection.getPriceLineId(),
        billLineCorrection.getMerchantCode(),
        billLineCorrection.getTotalAmount(),
        billLineCorrection.getTaxStatus(),
        billLineCorrection.getPreviousBillLine(),
        billLineCalculations,
        billLineServiceQuantities
    );
  }

}
