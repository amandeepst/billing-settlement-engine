package com.worldpay.pms.bse.engine.transformations.writers;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.worldpay.pms.bse.domain.common.Utils;
import com.worldpay.pms.bse.engine.BillingConfiguration.BillPriceWriterConfiguration;
import com.worldpay.pms.bse.engine.Encodings;
import com.worldpay.pms.bse.engine.transformations.model.billprice.BillPriceRow;
import com.worldpay.pms.spark.core.jdbc.JdbcWriter;
import com.worldpay.pms.spark.core.jdbc.JdbcWriterConfiguration;
import com.worldpay.pms.testing.junit.JdbcWriterBaseTest;
import com.worldpay.pms.testing.utils.DbUtils;
import java.math.BigDecimal;
import java.sql.Date;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import scala.Tuple2;

public class BillPriceWriterTest extends JdbcWriterBaseTest<Tuple2<String, BillPriceRow>> {

  public static final BillPriceRow BILL_PRICE_ROW = new BillPriceRow("PO453469873", "CHRG", "MREC0002",
      "PI_RECUR", BigDecimal.TEN, "EUR", "40530763222021-01-08NNNN", "Y", "REC_CHG",
      "103844035", "UVX-yYuv*E2@", "UQp0wPu3!;1K", "Ukj1~yum*w1D", Date.valueOf("2021-01-01"), "granularity");

  public static final BillPriceRow BILL_PRICE_ROW2 = new BillPriceRow("PO453469873", "CHRG", "MREC0002",
      "PI_RECUR", BigDecimal.TEN, "EUR", "40530763222021-01-08NNNN", "Y", "REC_CHG",
      "103844035", "UVX-yYuv*E2@", "UQp0wPu3!;1K", "Ukj1~yum*w1D", Date.valueOf("2021-01-01"), "granularity");


  @BeforeEach
  void setUp() {
    DbUtils.cleanUpWithoutMetadata(db, "bill_price");
  }

  @Test
  void canProcessNonEmptyPartition(SparkSession spark) {
    long count = write(spark.createDataset(provideSamples(), encoder()));

    assertThat(count, is(1L));
    assertCountIs("bill_price", 1L);
  }

  @Override
  protected void assertNoRowsWritten() {
    assertCountIs("bill_price", 0L);
  }

  @Override
  protected List<Tuple2<String, BillPriceRow>> provideSamples() {
    return new ArrayList<>(
        Arrays.asList(new Tuple2<>(Utils.generateId(), BILL_PRICE_ROW), new Tuple2<>(Utils.generateId(), BILL_PRICE_ROW2)));
  }

  @Override
  protected Encoder<Tuple2<String, BillPriceRow>> encoder() {
    return Encodings.BILL_PRICE_WITH_ID;
  }

  @Override
  protected JdbcWriter<Tuple2<String, BillPriceRow>> createWriter(JdbcWriterConfiguration jdbcWriterConfiguration) {
    BillPriceWriterConfiguration billPriceWriterConfiguration = new BillPriceWriterConfiguration();
    billPriceWriterConfiguration.setDataSource(jdbcWriterConfiguration.getDataSource());
    billPriceWriterConfiguration.setEnabled(jdbcWriterConfiguration.isEnabled());
    billPriceWriterConfiguration.setHints(jdbcWriterConfiguration.getHints());
    billPriceWriterConfiguration.setIdempotencyEnabled(jdbcWriterConfiguration.isIdempotencyEnabled());
    billPriceWriterConfiguration.setMaxBatchSize(jdbcWriterConfiguration.getMaxBatchSize());
    billPriceWriterConfiguration.setIdempotencyRegistry(jdbcWriterConfiguration.getIdempotencyRegistry());
    billPriceWriterConfiguration.setRepartitionTo(jdbcWriterConfiguration.getRepartitionTo());

    return new BillPriceWriter(billPriceWriterConfiguration);
  }

}