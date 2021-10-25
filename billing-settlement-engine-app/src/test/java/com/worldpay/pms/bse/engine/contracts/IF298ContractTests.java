package com.worldpay.pms.bse.engine.contracts;

import static com.worldpay.pms.spark.core.Resource.resourceAsString;

import com.google.common.collect.ImmutableMap;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

@Slf4j
public class IF298ContractTests extends DbContractTest{

  @Test
  void canRunInvoiceDataQuery() {
    String query = resourceAsString("contracts/IF298_invoice_data.sql")
        .replace("ODI_WORK.CM_INVOICE_DATA", "vw_invoice_data")
        .replace("ODI_WORK.CM_INV_DATA_EXCP", "vw_bill_error");

    String plan = runAndGetPlan("if298_invoice_data", query, ImmutableMap.of(
        "startDate", "2021/02/10 00:00:00",
        "endDate", "2021/02/11 00:00:00"
    ));

    log.info(plan);
  }

  @Test
  void canRunInvoiceDataLnQuery() {
    String query = resourceAsString("contracts/IF298_invoice_data_ln.sql")
        .replace("ODI_WORK.CM_INVOICE_DATA_LN", "vw_invoice_data_line");

    String plan = runAndGetPlan("if298_invoice_data_ln", query, ImmutableMap.of(
        "startDate", "2021/02/10 00:00:00",
        "endDate", "2021/02/11 00:00:00"
    ));

    log.info(plan);
  }

  @Test
  void canRunInvoiceDataLnBclQuery() {
    String query = resourceAsString("contracts/IF298_invoice_data_ln_bcl.sql")
        .replace("ODI_WORK.CM_INV_DATA_LN_BCL", "vw_invoice_data_line_bcl");

    String plan = runAndGetPlan("if298_invoice_data_ln_bcl", query, ImmutableMap.of(
        "startDate", "2021/02/10 00:00:00",
        "endDate", "2021/02/11 00:00:00"
    ));

    log.info(plan);
  }

  @Test
  void canRunInvoiceDataLnSvcQtyQuery() {
    String query = resourceAsString("contracts/IF298_invoice_data_line_svc_qty.sql")
        .replace("ODI_WORK.CM_INV_DATA_LN_SVC_QTY", "vw_invoice_data_line_svc_qty");

    String plan = runAndGetPlan("if298_invoice_data_ln_svc_qty", query, ImmutableMap.of(
        "startDate", "2021/02/10 00:00:00",
        "endDate", "2021/02/11 00:00:00"
    ));

    log.info(plan);
  }

  @Test
  void canRunInvoiceDataLnRateQuery() {
    String query = resourceAsString("contracts/IF298_invoice_data_ln_rate.sql")
        .replace("ODI_WORK.CM_INV_DATA_LN_RATE", "vw_invoice_data_line_rate");

    String plan = runAndGetPlan("if298_invoice_data_ln_rate", query, ImmutableMap.of(
        "startDate", "2021/02/10 00:00:00",
        "endDate", "2021/02/11 00:00:00"
    ));

    log.info(plan);
  }

  @Test
  void canRunInvoiceDataTaxQuery() {
    String query = resourceAsString("contracts/IF298_invoice_data_tax.sql")
        .replace("ODI_WORK.CM_INV_DATA_TAX", "vw_invoice_data_tax");

    String plan = runAndGetPlan("if298_invoice_data_tax", query, ImmutableMap.of(
        "startDate", "2021/02/10 00:00:00",
        "endDate", "2021/02/11 00:00:00"
    ));

    log.info(plan);
  }

  @Test
  void canRunInvoiceDataAdjQuery() {
    String query = resourceAsString("contracts/IF298_invoice_data_adj.sql")
        .replace("ODI_WORK.CM_INV_DATA_ADJ", "vw_invoice_data_adj");

    String plan = runAndGetPlan("if298_invoice_data_adj", query, ImmutableMap.of(
        "startDate", "2021/02/10 00:00:00",
        "endDate", "2021/02/11 00:00:00"
    ));

    log.info(plan);
  }

}
