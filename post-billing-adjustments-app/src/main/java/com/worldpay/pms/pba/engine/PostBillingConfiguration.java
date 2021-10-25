package com.worldpay.pms.pba.engine;

import com.typesafe.config.Optional;
import com.worldpay.pms.config.PmsConfiguration;
import com.worldpay.pms.spark.core.batch.BatchHistoryConfig;
import com.worldpay.pms.spark.core.jdbc.JdbcSourceConfiguration;
import com.worldpay.pms.spark.core.jdbc.JdbcWriterConfiguration;
import java.io.Serializable;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class PostBillingConfiguration implements PmsConfiguration {

  private int maxAttempts;
  private BatchHistoryConfig history;
  private double failedRowsThresholdPercent;

  @Optional
  private PublisherConfiguration publishers = new PublisherConfiguration();

  @Data
  public static class PublisherConfiguration {
    // insert hints:select hints
    private static final String PARALLEL_32 = "ENABLE_PARALLEL_DML PARALLEL(32):PARALLEL(32)";
    @Optional
    private boolean publishBillPayment = true;
    @Optional
    private String billDueDate = PARALLEL_32;
    @Optional
    private String billPaymentDetail = PARALLEL_32;
    @Optional
    private String billPaymentDetailSnapshot = PARALLEL_32;
  }

  private DataSources sources;
  private DataWriters writers;

  @Optional
  private Store store = new Store();

  @Data
  public static class DataSources {

    private JdbcSourceConfiguration withholdFunds;
    private JdbcSourceConfiguration bill;
  }

  @Data
  public static class DataWriters {
    private PostBillAdjustmentWriterConfiguration postBillAdjustment;
    private JdbcWriterConfiguration postBillAdjustmentAccounting;
  }

  @Data
  public static class PostBillAdjustmentWriterConfiguration extends JdbcWriterConfiguration {
    private static final int SOURCE_KEY_BATCH_SIZE = 10000;
    @Optional
    int sourceKeyIncrementBy = SOURCE_KEY_BATCH_SIZE;
    @Optional
    String sourceKeySequenceName = "CBE_PCE_OWNER.SOURCE_KEY_SQ";
  }

  @Data
  public static class Store implements Serializable {
    private static final int DEFAULT_MAX_ENTRIES = 100000;
    private static final int DEFAULT_CONCURRENCY = 4;
    private static final int DEFAULT_PRINT_STATS_INTERVAL= 50000;

    Integer maxEntries = DEFAULT_MAX_ENTRIES;
    Integer concurrency = DEFAULT_CONCURRENCY;
    Integer statsInterval = DEFAULT_PRINT_STATS_INTERVAL;

  }

}
