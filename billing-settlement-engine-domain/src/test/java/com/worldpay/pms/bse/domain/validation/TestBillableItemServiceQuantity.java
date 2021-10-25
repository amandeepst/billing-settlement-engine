package com.worldpay.pms.bse.domain.validation;

import com.worldpay.pms.bse.domain.model.BillableItemServiceQuantity;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class TestBillableItemServiceQuantity implements BillableItemServiceQuantity {

  private String rateSchedule;

  private Map<String, BigDecimal> serviceQuantities;

  public static TestBillableItemServiceQuantity getBadFundingFxSvcQty() {
    return TestBillableItemServiceQuantity.builder()
        .rateSchedule("CHGPPIBL")
        .serviceQuantities(new HashMap<String, BigDecimal>() {{
          put("F_M_AMT", new BigDecimal("100"));
          put("F_B_MFX", new BigDecimal("1.03"));
        }})
        .build();
  }

  public static TestBillableItemServiceQuantity getBadFundingAmountSvcQty() {
    return TestBillableItemServiceQuantity.builder()
        .rateSchedule("CHGPPIBL")
        .serviceQuantities(new HashMap<String, BigDecimal>() {{
          put("F_M_AMT", new BigDecimal("200"));
          put("F_B_MFX", new BigDecimal("1.05"));
        }})
        .build();
  }

  public static TestBillableItemServiceQuantity getBadChargingFxSvcQty() {
    return TestBillableItemServiceQuantity.builder()
        .rateSchedule("CHGPPIBL")
        .serviceQuantities(new HashMap<String, BigDecimal>() {{
          put("TXN_VOL", new BigDecimal("10"));
          put("F_M_AMT", new BigDecimal("100"));
          put("F_B_MFX", new BigDecimal("1.05"));
        }})
        .build();
  }

  public static TestBillableItemServiceQuantity getBadAlternatePricingFxSvcQty() {
    return TestBillableItemServiceQuantity.builder()
        .rateSchedule("CHGICPAP")
        .serviceQuantities(new HashMap<String, BigDecimal>() {{
          put("TXN_VOL", new BigDecimal("10"));
          put("F_M_AMT", new BigDecimal("100"));
          put("F_B_MFX", new BigDecimal("1.03"));
        }})
        .build();
  }

  public static TestBillableItemServiceQuantity getCorrectSvcQty() {
    return TestBillableItemServiceQuantity.builder()
        .rateSchedule("CHGPPIBL")
        .serviceQuantities(new HashMap<String, BigDecimal>() {{
          put("TXN_VOL", new BigDecimal("10"));
          put("F_M_AMT", new BigDecimal("100"));
          put("F_B_MFX", new BigDecimal("1.05"));
          put("P_B_EFX", new BigDecimal("1.02"));
        }})
        .build();
  }

  public static TestBillableItemServiceQuantity getCorrectBigAmountSvcQty() {
    return TestBillableItemServiceQuantity.builder()
        .rateSchedule("CHGPPIBL")
        .serviceQuantities(new HashMap<String, BigDecimal>() {{
          put("TXN_VOL", new BigDecimal("10002"));
          put("F_M_AMT", new BigDecimal("946666.4"));
          put("F_B_MFX", new BigDecimal("1.00"));
          put("P_B_EFX", new BigDecimal("1.00"));
        }})
        .build();
  }
}
