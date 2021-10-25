package com.worldpay.pms.bse.domain.validation;

import com.worldpay.pms.bse.domain.model.BillableItem;
import com.worldpay.pms.bse.domain.model.BillableItemLine;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DefaultCalculationCheckService implements CalculationCheckService {

  private static final List<String> ALTERNATE_PRICING_SCHEDULES = Arrays
      .asList("CHGASFAP", "CHGICPAP", "CHGPIAP", "CHGPPIAP", "CHICPPAP", "CHGASFRA");
  private static final double TOLERANCE_PC = 0.01 / 100;
  private static final double TOLERANCE_ABS = 0.01;
  private static final int RATE_ROUNDING_SCALE = 6;
  private static final int ONE_HUNDRED = 100;

  @Override
  public Boolean apply(BillableItem billableItem) {

    Map<String, BigDecimal> serviceQuantities = billableItem.getBillableItemServiceQuantity().getServiceQuantities();
    String rateSchedule = billableItem.getBillableItemServiceQuantity().getRateSchedule();

    for (BillableItemLine line : billableItem.getBillableItemLines()) {
      if (!isLineCalculationCorrect(line, rateSchedule, serviceQuantities)) {
        log.error("Miscalculation detected for billableItemId={}", billableItem.getBillableItemId());
        return false;
      }
    }

    return true;
  }

  private Boolean isLineCalculationCorrect(BillableItemLine line, String rateSchedule, Map<String, BigDecimal> serviceQuantities) {

    if (line.getCalculationLineType() != null && "PC_MBA".equals(line.getCalculationLineType())) {
      BigDecimal comparisonResult = line.getAmount().subtract(serviceQuantities.get("F_M_AMT").multiply(line.getRateValue())
          .multiply(serviceQuantities.getOrDefault("F_B_MFX", BigDecimal.ONE)));

      double absoluteTolerance =
          line.getAmount().doubleValue() > ONE_HUNDRED ? (line.getAmount().doubleValue() * TOLERANCE_PC) : TOLERANCE_ABS;

      return comparisonResult.doubleValue() <= absoluteTolerance && comparisonResult.doubleValue() >= -absoluteTolerance;
    }
    if (line.getCalculationLineType() != null && "PI_MBA".equals(line.getCalculationLineType())) {
      BigDecimal fxRate = ALTERNATE_PRICING_SCHEDULES.contains(rateSchedule) ? serviceQuantities.getOrDefault("AP_B_EFX", BigDecimal.ONE)
          : serviceQuantities.getOrDefault("P_B_EFX", BigDecimal.ONE);

      int comparisonResult = line.getAmount().compareTo(serviceQuantities.get("TXN_VOL")
          .multiply(line.getRateValue().multiply(fxRate).setScale(RATE_ROUNDING_SCALE, RoundingMode.HALF_UP)));

      return comparisonResult == 0;
    }
    return true;
  }
}
