package com.worldpay.pms.bse.engine.transformations.model.pendingbill;

import com.worldpay.pms.bse.domain.model.PendingBillLine;
import com.worldpay.pms.bse.engine.transformations.model.completebill.BillLineDetail;
import com.worldpay.pms.bse.engine.transformations.model.input.pending.InputPendingBillLineRow;
import com.worldpay.pms.bse.engine.transformations.model.pendingbill.aggregation.PendingBillLineAggKey;
import java.math.BigDecimal;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder(toBuilder = true)
public class PendingBillLineRow implements PendingBillLine {

  private String billLineId;
  private String billLinePartyId;
  private String productClass;
  private String productIdentifier;
  private String pricingCurrency;
  private String fundingCurrency;
  private BigDecimal fundingAmount;
  private String transactionCurrency;
  private BigDecimal transactionAmount;
  private BigDecimal quantity;
  private String priceLineId;
  private String merchantCode;
  private long partitionId;

  private BillLineDetail[] billLineDetails;
  private PendingBillLineCalculationRow[] pendingBillLineCalculations;
  private PendingBillLineServiceQuantityRow[] pendingBillLineServiceQuantities;

  public static PendingBillLineRow from(InputPendingBillLineRow raw, PendingBillLineCalculationRow[] pendingBillLineCalculations,
      PendingBillLineServiceQuantityRow[] pendingBillLineServiceQuantities) {

    return new PendingBillLineRow(
        raw.getBillLineId(),
        raw.getBillLinePartyId(),
        raw.getProductClass(),
        raw.getProductIdentifier(),
        raw.getPricingCurrency(),
        raw.getFundingCurrency(),
        raw.getFundingAmount(),
        raw.getTransactionCurrency(),
        raw.getTransactionAmount(),
        raw.getQuantity(),
        raw.getPriceLineId(),
        raw.getMerchantCode(),
        raw.getPartitionId(),
        new BillLineDetail[0],
        pendingBillLineCalculations,
        pendingBillLineServiceQuantities
    );
  }

  public PendingBillLineAggKey aggregationKey() {
    return new PendingBillLineAggKey(
        billLinePartyId,
        productIdentifier,
        pricingCurrency,
        fundingCurrency,
        transactionCurrency,
        priceLineId,
        merchantCode
    );
  }
}
