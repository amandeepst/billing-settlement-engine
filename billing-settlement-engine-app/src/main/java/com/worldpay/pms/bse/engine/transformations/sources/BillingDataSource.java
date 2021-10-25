package com.worldpay.pms.bse.engine.transformations.sources;

import static com.worldpay.pms.spark.core.Resource.resourceAsString;
import static com.worldpay.pms.spark.core.jdbc.JdbcDataSource.PartitionParameters.byColumn;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.worldpay.pms.spark.core.batch.Batch.BatchId;
import com.worldpay.pms.spark.core.jdbc.JdbcDataSource;
import com.worldpay.pms.spark.core.jdbc.JdbcSourceConfiguration;
import java.io.Serializable;
import java.sql.Timestamp;
import java.util.Map;
import org.apache.spark.sql.Encoder;

public class BillingDataSource<T> extends JdbcDataSource.For<T> {

  private final String queryPath;
  private final JdbcSourceConfiguration conf;
  private final int maxAttempts;

  protected BillingDataSource(String name, String queryPath, JdbcSourceConfiguration conf, Encoder<T> encoder) {
    super(name, conf, encoder);
    this.queryPath = queryPath;
    this.conf = conf;
    this.maxAttempts = -1;
  }

  protected BillingDataSource(String name, String queryPath, JdbcSourceConfiguration conf, int maxAttempts, Encoder<T> encoder) {
    super(name, conf, encoder);
    this.queryPath = queryPath;
    this.conf = conf;
    this.maxAttempts = maxAttempts;
  }

  @Override
  protected String sql(BatchId batchId) {
    return resourceAsString(queryPath)
        .replace(":hints", conf.getHints());
  }

  @Override
  protected Map<String, Serializable> getBindArguments(BatchId batchId) {
    String rawQuery = sql(batchId);

    Builder<String, Serializable> bindArguments = ImmutableMap.builder();

    if (rawQuery.contains(":low")) {
      bindArguments.put("low", Timestamp.valueOf(batchId.watermark.low));
    }

    if (rawQuery.contains(":high")) {
      bindArguments.put("high", Timestamp.valueOf(batchId.watermark.high));
    }

    if (rawQuery.contains(":partitions")) {
      bindArguments.put("partitions", String.valueOf(conf.getPartitionHighBound()));
    }

    if (rawQuery.contains(":maxAttempts")) {
      bindArguments.put("maxAttempts", maxAttempts);
    }

    return bindArguments.build();
  }

  @Override
  protected PartitionParameters getPartitionParameters() {
    return byColumn("partitionId", conf.getPartitionCount(), conf.getPartitionHighBound());
  }
}
