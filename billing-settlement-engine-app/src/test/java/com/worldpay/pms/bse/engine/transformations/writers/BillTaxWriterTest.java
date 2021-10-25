package com.worldpay.pms.bse.engine.transformations.writers;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.worldpay.pms.bse.domain.model.billtax.BillLineTax;
import com.worldpay.pms.bse.domain.model.billtax.BillTax;
import com.worldpay.pms.bse.domain.model.billtax.BillTaxDetail;
import com.worldpay.pms.bse.engine.Encodings;
import com.worldpay.pms.spark.core.jdbc.JdbcWriter;
import com.worldpay.pms.spark.core.jdbc.JdbcWriterConfiguration;
import com.worldpay.pms.testing.junit.JdbcWriterBaseTest;
import com.worldpay.pms.testing.utils.DbUtils;
import java.math.BigDecimal;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import lombok.val;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class BillTaxWriterTest extends JdbcWriterBaseTest<BillTax> {

  private static BillTax billTax;

  @BeforeAll
  static void init() {
    BillLineTax billLineTax1 = new BillLineTax("EXE", BigDecimal.ZERO, "E Exempt", BigDecimal.TEN, BigDecimal.ZERO);
    BillLineTax billLineTax2 = new BillLineTax("S", BigDecimal.valueOf(21), "STANDARD-S", BigDecimal.TEN, BigDecimal.valueOf(2.1));
    BillLineTax billLineTax3 = new BillLineTax("S", BigDecimal.valueOf(21), "STANDARD-S", BigDecimal.valueOf(20), BigDecimal.valueOf(4.2));

    HashMap<String, BillLineTax> billLineTaxData = new HashMap<>();
    billLineTaxData.put("bill_line_id_1", billLineTax1);
    billLineTaxData.put("bill_line_id_2", billLineTax2);
    billLineTaxData.put("bill_line_id_3", billLineTax3);

    val billTaxId = "tax_id_1";

    BillTaxDetail billTaxDetail1 = new BillTaxDetail(billTaxId, "EXE", BigDecimal.ZERO, "E Exempt", BigDecimal.TEN, BigDecimal.ZERO);
    BillTaxDetail billTaxDetail2 = new BillTaxDetail(billTaxId, "S", BigDecimal.valueOf(21), "STANDARD-S", BigDecimal.valueOf(30),
        BigDecimal.valueOf(6.3));

    billTax = new BillTax(billTaxId, "bill_id_1", "GB991280207", "wp_reg_1", "VAT", "HMRC", "N", billLineTaxData,
        new BillTaxDetail[]{billTaxDetail1, billTaxDetail2});
  }

  @BeforeEach
  void cleanup() {
    DbUtils.cleanUp(db, "bill_tax", "bill_tax_detail");
  }

  @Test
  void canProcessNonEmptyPartition(SparkSession sparkSession) {
    long count = writer.write(batchId, NOW, sparkSession.createDataset(provideSamples(), encoder()));

    assertThat(count, is(1L));
    assertRowsWritten(1, 2);
  }

  @Override
  protected void assertNoRowsWritten() {
    assertRowsWritten(0, 0);
  }

  @Override
  protected List<BillTax> provideSamples() {
    return Collections.singletonList(billTax);
  }

  @Override
  protected Encoder<BillTax> encoder() {
    return Encodings.BILL_TAX;
  }

  @Override
  protected JdbcWriter<BillTax> createWriter(JdbcWriterConfiguration jdbcWriterConfiguration) {
    return new BillTaxWriter(jdbcWriterConfiguration);
  }

  private void assertRowsWritten(long expectedBillTaxCount, long expectedBillTaxDetailCount) {
    assertCountIs("bill_tax", expectedBillTaxCount);
    assertCountIs("bill_tax_detail", expectedBillTaxDetailCount);
  }
}