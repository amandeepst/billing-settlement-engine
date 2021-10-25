package com.worldpay.pms.bse.engine.transformations.writers;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.worldpay.pms.bse.engine.Encodings;
import com.worldpay.pms.bse.engine.transformations.model.billrelationship.BillRelationshipRow;
import com.worldpay.pms.config.ApplicationConfiguration;
import com.worldpay.pms.spark.core.jdbc.JdbcWriter;
import com.worldpay.pms.spark.core.jdbc.JdbcWriterConfiguration;
import com.worldpay.pms.testing.junit.JdbcWriterBaseTest;
import com.worldpay.pms.testing.utils.DbUtils;
import java.util.Collections;
import java.util.List;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class BillRelationshipWriterTest extends JdbcWriterBaseTest<BillRelationshipRow> {

  public static final BillRelationshipRow BILL_RELATIONSHIP_ROW = new BillRelationshipRow("parentId","childId",
      "12345678", "51", "N", "N","CANCEL","DPLC");

  @BeforeEach
  void setUp() {
    DbUtils.cleanUp(db, "bill_relationship");
  }

  @Test
  void canProcessNonEmptyPartition(ApplicationConfiguration conf, SparkSession spark) {
    long count = write(spark.createDataset(provideSamples(), encoder()));

    assertThat(count, is(1L));
    assertCountIs("bill_relationship", 1L);
  }

  @Override
  protected void assertNoRowsWritten() {
    assertCountIs("bill_relationship", 0L);
  }

  @Override
  protected List<BillRelationshipRow> provideSamples() {
    return Collections.singletonList(BILL_RELATIONSHIP_ROW);
  }

  @Override
  protected Encoder<BillRelationshipRow> encoder() {
    return Encodings.BILL_RELATIONSHIP_ENCODER;
  }

  @Override
  protected JdbcWriter<BillRelationshipRow> createWriter(JdbcWriterConfiguration jdbcWriterConfiguration) {
    return new BillRelationshipWriter(jdbcWriterConfiguration);
  }


}
