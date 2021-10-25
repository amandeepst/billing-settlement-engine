package com.worldpay.pms.pba.engine.transformations.sources;

import static com.worldpay.pms.spark.core.Resource.resourceAsString;
import static com.worldpay.pms.spark.core.jdbc.JdbcDataSource.PartitionParameters.byColumn;

import com.google.common.collect.ImmutableMap;
import com.worldpay.pms.pba.engine.Encodings;
import com.worldpay.pms.pba.engine.model.input.BillRow;
import com.worldpay.pms.spark.core.batch.Batch.BatchId;
import com.worldpay.pms.spark.core.jdbc.JdbcDataSource;
import com.worldpay.pms.spark.core.jdbc.JdbcSourceConfiguration;
import java.io.Serializable;
import java.sql.Timestamp;
import java.util.Map;

public class BillSource extends JdbcDataSource.For<BillRow> {

  private static final String QUERY = resourceAsString("sql/input/get-bills.sql");
  private final JdbcSourceConfiguration conf;

  public BillSource(JdbcSourceConfiguration conf) {
    super("bill", conf, Encodings.BILL_ROW_ENCODER);
    this.conf = conf;
  }

  @Override
  protected String sql(BatchId batchId) {
    return QUERY
        .replace(":hints", conf.getHints());
  }

  @Override
  protected Map<String, Serializable> getBindArguments(BatchId batchId) {
    return ImmutableMap.<String, Serializable>builder()
        .put("low", Timestamp.valueOf(batchId.watermark.low))
        .put("high", Timestamp.valueOf(batchId.watermark.high))
        .build();
  }

  @Override
  protected PartitionParameters getPartitionParameters() {
    return byColumn("partitionId", conf.getPartitionCount(), conf.getPartitionHighBound());
  }
}
