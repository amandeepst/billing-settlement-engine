package com.worldpay.pms.bse.engine.transformations.writers;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.worldpay.pms.bse.engine.Encodings;
import com.worldpay.pms.bse.engine.transformations.model.billableitem.BillableItemLineRow;
import com.worldpay.pms.bse.engine.transformations.model.billableitem.BillableItemRow;
import com.worldpay.pms.bse.engine.transformations.model.billableitem.BillableItemServiceQuantityRow;
import com.worldpay.pms.bse.engine.transformations.model.failedbillableitem.FailedBillableItem;
import com.worldpay.pms.pce.common.DomainError;
import com.worldpay.pms.spark.core.jdbc.JdbcWriter;
import com.worldpay.pms.spark.core.jdbc.JdbcWriterConfiguration;
import com.worldpay.pms.testing.junit.JdbcWriterBaseTest;
import com.worldpay.pms.testing.utils.DbUtils;
import java.math.BigDecimal;
import java.sql.Date;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.val;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class FailedBillableItemWriterTest extends JdbcWriterBaseTest<FailedBillableItem> {

  private static final String ERROR_TABLE = "bill_item_error";

  @BeforeEach
  void setUp() {
    DbUtils.cleanUp(db, ERROR_TABLE);
  }

  @Test
  void canWriteNonEmptyPartition(SparkSession spark) {
    write(spark.createDataset(provideSamples(), encoder()));

    assertRowsWritten(2L);
  }

  private void assertRowsWritten(long expectedFailedBillableItemCount) {
    assertThat(getFailedBillableItemsCount(), is(expectedFailedBillableItemCount));
  }

  private long getFailedBillableItemsCount() {
    return DbUtils.getCount(db, ERROR_TABLE);
  }

  @Override
  protected void assertNoRowsWritten() {
    assertRowsWritten(0);
  }

  @Override
  protected List<FailedBillableItem> provideSamples() {
    return getTestBillableItems()
        .stream()
        .map(item -> FailedBillableItem.of(item,
            io.vavr.collection.List.of(DomainError.of("EROR", "Some Reason for " + item.getBillableItemId()))))
        .collect(Collectors.toList());
  }

  @Override
  protected Encoder<FailedBillableItem> encoder() {
    return Encodings.FAILED_BILLABLE_ITEM_ENCODER;
  }

  @Override
  protected JdbcWriter<FailedBillableItem> createWriter(JdbcWriterConfiguration jdbcWriterConfiguration) {
    return new FailedBillableItemWriter(jdbcWriterConfiguration, SparkSession.builder().getOrCreate());
  }

  private List<BillableItemRow> getTestBillableItems() {
    Map<String, BigDecimal> serviceQuantities = new HashMap<>();
    serviceQuantities.put("TXN_VOL", BigDecimal.ONE);
    BillableItemServiceQuantityRow billableItemServiceQuantityRow = new BillableItemServiceQuantityRow("rateSchedule", serviceQuantities);

    BillableItemLineRow billableItemLineRow = new BillableItemLineRow(
        "class_1",
        BigDecimal.ONE,
        "F_M_AMT",
        "N/A",
        BigDecimal.ONE,
        1
    );

    val billableItemRow1 = new BillableItemRow(
        "testId1",
        "test",
        "lcp",
        Date.valueOf("2021-01-01"),
        "0101010101",
        "sett_lvl_type|sett_lvl_val",
        "gran_kv",
        "EUR",
        "EUR",
        "EUR",
        "EUR",
        "EUR",
        "1849326380",
        "PREMIUM",
        "PRVCCL01",
        0,
        null,
        "123",
        -1,
        "N",
        "STANDARD_BILLABLE_ITEM",
        "N",
        "N",
        "N",
        "N",
        "N",
        "N",
        "N",
        null,
        null,
        null,
        null,
        Date.valueOf(LocalDate.now()),
        0,
        Date.valueOf("2021-01-01"),
        new BillableItemLineRow[]{billableItemLineRow},
        billableItemServiceQuantityRow);

    val billableItemRow2 = new BillableItemRow(
        "testId2",
        "test",
        "lcp",
        Date.valueOf("2021-01-01"),
        "0101010102",
        "invalid_sett_val",
        "gran_kv",
        "EUR",
        "EUR",
        "EUR",
        "EUR",
        "EUR",
        "1849326380",
        "PREMIUM",
        "PRVCCL01",
        0,
        null,
        "123",
        -1,
        "N",
        "STANDARD_BILLABLE_ITEM",
        "N",
        "N",
        "N",
        "N",
        "N",
        "N",
        "N",
        null,
        null,
        null,
        null,
        Date.valueOf(LocalDate.now()),
        0,
        Date.valueOf("2021-01-01"),
        new BillableItemLineRow[]{billableItemLineRow},
        billableItemServiceQuantityRow);

    return Arrays.asList(billableItemRow1, billableItemRow2);
  }

}