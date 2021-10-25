package com.worldpay.pms.bse.domain.model.completebill;

import com.worldpay.pms.bse.domain.common.Utils;
import com.worldpay.pms.bse.domain.model.PendingBillLine;
import com.worldpay.pms.bse.domain.model.mincharge.MinimumCharge;
import io.vavr.collection.List;
import io.vavr.collection.Seq;
import java.math.BigDecimal;
import java.util.function.UnaryOperator;
import lombok.Builder;
import lombok.Value;
import lombok.With;

@With
@Builder
@Value
public class CompleteBillLine {

  String billLineId;
  String billLinePartyId;
  String productClass;
  String productIdentifier;
  String pricingCurrency;
  String fundingCurrency;
  BigDecimal fundingAmount;
  String transactionCurrency;
  BigDecimal transactionAmount;
  BigDecimal quantity;
  String priceLineId;
  String merchantCode;
  BigDecimal totalAmount;

  CompleteBillLineCalculation[] billLineCalculations;
  CompleteBillLineServiceQuantity[] billLineServiceQuantities;

  public static CompleteBillLine from(PendingBillLine pendingBillLine, UnaryOperator<BigDecimal> rounder) {
    Seq<CompleteBillLineCalculation> completeBillLinesCalculations = List.of(pendingBillLine.getPendingBillLineCalculations())
        .map(calc -> CompleteBillLineCalculation.from(calc, rounder));

    BigDecimal amount = completeBillLinesCalculations.map(CompleteBillLineCalculation::getAmount).reduce(BigDecimal::add);
    BigDecimal computedTotalAmount = rounder.apply(amount);

    return new CompleteBillLine(
        pendingBillLine.getBillLineId(),
        pendingBillLine.getBillLinePartyId(),
        pendingBillLine.getProductClass(),
        pendingBillLine.getProductIdentifier(),
        pendingBillLine.getPricingCurrency(),
        pendingBillLine.getFundingCurrency(),
        pendingBillLine.getFundingAmount(),
        pendingBillLine.getTransactionCurrency(),
        pendingBillLine.getTransactionAmount(),
        pendingBillLine.getQuantity(),
        pendingBillLine.getPriceLineId(),
        pendingBillLine.getMerchantCode(),
        computedTotalAmount,
        completeBillLinesCalculations.toJavaArray(CompleteBillLineCalculation[]::new),
        List.of(pendingBillLine.getPendingBillLineServiceQuantities())
            .map(svcQty -> CompleteBillLineServiceQuantity.from(svcQty, rounder))
            .toJavaArray(CompleteBillLineServiceQuantity[]::new)
    );
  }

  public static CompleteBillLine from(MinimumCharge minimumCharge, BigDecimal minChargeAmount, UnaryOperator<BigDecimal> rounder) {

    return new CompleteBillLine(
        Utils.generateId(),
        minimumCharge.getPartyId(),
        "MISCELLANEOUS",
        "MINCHRGP",
        minimumCharge.getCurrency(),
        null,
        null,
        null,
        null,
        BigDecimal.ONE,
        minimumCharge.getPriceAssignId(),
        null,
        rounder.apply(minChargeAmount),
        new CompleteBillLineCalculation[]{CompleteBillLineCalculation.from(minimumCharge, minChargeAmount, rounder)},
        List.of(CompleteBillLineServiceQuantity.builder().serviceQuantityTypeCode("TXN_VOL").serviceQuantityValue(BigDecimal.ONE).build())
            .toJavaArray(CompleteBillLineServiceQuantity[]::new)
    );
  }
}
