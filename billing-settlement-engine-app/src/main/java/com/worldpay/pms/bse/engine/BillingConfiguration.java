package com.worldpay.pms.bse.engine;

import com.typesafe.config.Optional;
import com.worldpay.pms.config.PmsConfiguration;
import com.worldpay.pms.spark.core.batch.BatchHistoryConfig;
import com.worldpay.pms.spark.core.jdbc.JdbcConfiguration;
import com.worldpay.pms.spark.core.jdbc.JdbcSourceConfiguration;
import com.worldpay.pms.spark.core.jdbc.JdbcWriterConfiguration;
import java.io.Serializable;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class BillingConfiguration implements PmsConfiguration {

  public static final String NO_HINTS = "";

  private int maxAttempts;
  private BatchHistoryConfig history;

  private double failedRowsThresholdPercent;
  @Optional
  private long failedRowsThresholdCount = Long.MAX_VALUE;
  @Optional
  private boolean publishOnThresholdFailure = false;

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

  @Optional
  private FailedBillsThreshold failedBillsThreshold;

  @Data
  public static class FailedBillsThreshold {

    @Optional
    private double failedBillRowsThresholdPercent = -1.0D;
    @Optional
    private long failedBillRowsThresholdCount = Long.MAX_VALUE;
  }

  @Optional
  private DriverConfig driverConfig = new DriverConfig();

  @Optional
  private Billing billing = new Billing();

  @Optional
  private BillIdSeqConfiguration billIdSeqConfiguration = new BillIdSeqConfiguration();

  private DataSources sources;
  private DataWriters writers;

  @Data
  public static class DataSources {

    private BillableItemSourceConfiguration billableItems;
    private MiscBillableItemSourceConfiguration miscBillableItems;
    private FailedBillableItemSourceConfiguration failedBillableItems;

    private JdbcSourceConfiguration minCharge;
    private JdbcSourceConfiguration pendingMinCharge;

    private PendingConfiguration pendingConfiguration;
    private BillCorrectionSourceConfiguration billCorrectionConfiguration;

    private AccountRepositoryConfiguration accountData;
    private StaticDataRepositoryConfiguration staticData;

    private JdbcSourceConfiguration billTax;
  }

  @Data
  public static class DataWriters {

    private JdbcWriterConfiguration pendingBill;
    private JdbcWriterConfiguration completeBill;
    private JdbcWriterConfiguration billLineDetail;
    private JdbcWriterConfiguration billError;
    private JdbcWriterConfiguration failedBillableItem;
    private BillPriceWriterConfiguration billPrice;
    private JdbcWriterConfiguration billTax;
    private JdbcWriterConfiguration pendingMinCharge;
    private JdbcWriterConfiguration billAccounting;
    private JdbcWriterConfiguration billRelationship;
    private JdbcWriterConfiguration minChargeBills;
  }


  @Data
  public static class BillableItemSourceConfiguration extends JdbcSourceConfiguration {

  }

  @Data
  public static class MiscBillableItemSourceConfiguration extends JdbcSourceConfiguration {

  }

  @Data
  public static class FailedBillableItemSourceConfiguration extends JdbcSourceConfiguration {

  }

  @Data
  public static class BillPriceWriterConfiguration extends JdbcWriterConfiguration {
    private static final int SOURCE_KEY_BATCH_SIZE = 10000;
    @Optional
    int sourceKeyIncrementBy = SOURCE_KEY_BATCH_SIZE;
    @Optional
    String sourceKeySequenceName = "CBE_PCE_OWNER.SOURCE_KEY_SQ";
    @Optional
    String nonEventIdSequenceName = "CBE_BSE_OWNER.NON_EVENT_ID_SQ";
  }

  @Data
  public static class BillIdSeqConfiguration extends JdbcSourceConfiguration implements Serializable {
    private static final int BILL_ID_BATCH_SIZE = 1000;
    @Optional
    int billIdIncrementBy = BILL_ID_BATCH_SIZE;
    @Optional
    String billIdSequenceName = "CBE_BSE_OWNER.BILL_ID_SQ";
  }

  @Data
  public static class PendingConfiguration {

    private JdbcSourceConfiguration pendingBill;
    private JdbcSourceConfiguration pendingBillLine;
    private JdbcSourceConfiguration pendingBillLineCalc;
    private JdbcSourceConfiguration pendingBillLineSvcQty;

  }

  @Data
  public static class BillCorrectionSourceConfiguration {

    private JdbcSourceConfiguration billCorrection;
    private JdbcSourceConfiguration billLineCorrection;
    private JdbcSourceConfiguration billLineCalcCorrection;
    private JdbcSourceConfiguration billLineSvcQtyCorrection;
  }

  @Data
  public static class AccountRepositoryConfiguration implements Serializable {

    @Optional
    String accountDetailsHints = NO_HINTS;

    @Optional
    private JdbcConfiguration conf = new JdbcConfiguration();
  }

  @Data
  public static class StaticDataRepositoryConfiguration implements Serializable {

    @Optional
    private JdbcConfiguration conf = new JdbcConfiguration();
  }

  @Data
  public static class Billing implements Serializable {

    private static final int DEFAULT_CONCURRENCY = 4;
    private static final int DEFAULT_PRINT_STATS_INTERVAL = 50000;
    private static final int DEFAULT_ACCOUNTS_LIST_MAX_ENTRIES = 2000000;
    private static final int DEFAULT_MIN_CHARGE_LIST_MAX_ENTRIES = 500000;

    Integer accountsListMaxEntries = DEFAULT_ACCOUNTS_LIST_MAX_ENTRIES;
    Integer accountsListConcurrency = DEFAULT_CONCURRENCY;
    Integer accountsListStatsInterval = DEFAULT_PRINT_STATS_INTERVAL;

    Integer minChargeListMaxEntries = DEFAULT_MIN_CHARGE_LIST_MAX_ENTRIES;
    Integer minChargeListConcurrency = DEFAULT_CONCURRENCY;
    Integer minChargeListStatsInterval = DEFAULT_PRINT_STATS_INTERVAL;
  }

  @Data
  public static class DriverConfig implements Serializable{
    private static final String DEFAULT_COMPLETE_BILL_STORAGE = "MEMORY_AND_DISK";

    String completeBillStorageLevel = DEFAULT_COMPLETE_BILL_STORAGE;
  }
}
