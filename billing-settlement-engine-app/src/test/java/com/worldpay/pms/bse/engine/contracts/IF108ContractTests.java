package com.worldpay.pms.bse.engine.contracts;

import static com.worldpay.pms.spark.core.Resource.resourceAsString;

import com.google.common.collect.ImmutableMap;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

@Slf4j
public class IF108ContractTests extends DbContractTest{

  @Test
  void canRunPaymentRequestQuery() {
    String query = resourceAsString("contracts/if108/if108_payment_request.sql")
        .replace("ODI_WORK.CM_PAY_REQ", "vw_payment_request");

    String plan = runAndGetPlan("if108_payment_request", query, ImmutableMap.of(
        "startDate", "2021 04 01 00:00:00",
        "endDate", "2021 04 05 00:00:00"
    ));

    log.info(plan);
  }

  @Test
  void canRunPaymentRequestGranularityQuery() {
    String query = resourceAsString("contracts/if108/if108_payment_request_granularity.sql")
        .replace("ODI_WORK.CM_PAY_REQ_GRANULARITIES", "vw_payment_request_granularity");

    String plan = runAndGetPlan("if108_payment_request_granularity", query, ImmutableMap.of(
        "startDate", "2021 04 01 00:00:00",
        "endDate", "2021 04 05 00:00:00"
    ));

    log.info(plan);
  }
}
