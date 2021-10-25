package com.worldpay.pms.bse.engine.transformations.sources;

import static com.worldpay.pms.spark.core.Resource.resourceAsString;
import static com.worldpay.pms.spark.core.jdbc.JdbcDataSource.PartitionParameters.byColumn;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.worldpay.pms.bse.engine.Encodings;
import com.worldpay.pms.bse.engine.InputEncodings;
import com.worldpay.pms.bse.engine.transformations.model.input.mincharge.MinimumChargeBillRow;
import com.worldpay.pms.bse.engine.transformations.model.input.mincharge.PendingMinChargeRow;
import com.worldpay.pms.spark.core.DataSource;
import com.worldpay.pms.spark.core.batch.Batch.BatchId;
import com.worldpay.pms.spark.core.jdbc.JdbcDataSource;
import com.worldpay.pms.spark.core.jdbc.JdbcSourceConfiguration;
import java.io.Serializable;
import java.sql.Date;
import java.time.LocalDate;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.SparkSession;

public class PendingMinChargeSource implements DataSource<PendingMinChargeRow> {

  private static final String QUERY_PATH = "sql/inputs/get-pending-min-charge.sql";
  public static final String QUERY_MIN_CHARGE_AS_PENDING = "sql/inputs/get-min-charge-as-pending.sql";
  public static final String QUERY_MIN_CHARGE_BILLS = "sql/inputs/get-min-charge-bill.sql";

  private BillingDataSource<PendingMinChargeRow> pendingMinChargeDataSource;
  private MinChargeSource<PendingMinChargeRow> minChargeSource;
  private MinChargeSource<MinimumChargeBillRow> minChargeBillSource;

  public PendingMinChargeSource(JdbcSourceConfiguration conf, JdbcSourceConfiguration minChargeConf, LocalDate logicalDate) {
    pendingMinChargeDataSource = new BillingDataSource<>("pending-min-charge", QUERY_PATH, conf,
        InputEncodings.PENDING_MIN_CHARGE_ROW_ENCODER);
    minChargeSource = new MinChargeSource("min-charge", minChargeConf, InputEncodings.PENDING_MIN_CHARGE_ROW_ENCODER,
        logicalDate, QUERY_MIN_CHARGE_AS_PENDING);
    minChargeBillSource = new MinChargeSource("min-charge-bill", conf, InputEncodings.MIN_CHARGE_BILL_ROW_ENCODER,
        logicalDate, QUERY_MIN_CHARGE_BILLS);
  }

  @Override
  public Dataset<PendingMinChargeRow> load(SparkSession session, BatchId batchId) {

    Dataset<PendingMinChargeRow> minChargeDataset = minChargeSource.load(session, batchId)
        .dropDuplicates("billPartyId", "legalCounterparty", "txnPartyId", "minChargeType", "currency");
    Dataset<MinimumChargeBillRow> minChargeBillDataset = minChargeBillSource.load(session, batchId);

    Dataset<PendingMinChargeRow> minChargeAsPendingDataset = minChargeDataset
        .join(minChargeBillDataset, minChargeDataset.col("billPartyId").equalTo(minChargeBillDataset.col("billPartyId"))
            .and(minChargeDataset.col("legalCounterparty").equalTo(minChargeBillDataset.col("legalCounterparty")))
            .and(minChargeDataset.col("currency").equalTo(minChargeBillDataset.col("currency"))), "left_anti")
        .as(Encodings.PENDING_MINIMUM_CHARGE_ROW_ENCODER);

    return pendingMinChargeDataSource.load(session, batchId)
        .union(minChargeAsPendingDataset);
  }

  public static class MinChargeSource<T> extends JdbcDataSource.For<T> implements DataSource<T> {

    private JdbcSourceConfiguration conf;
    private LocalDate logicalDate;
    private String queryPath;

    protected MinChargeSource(String name, JdbcSourceConfiguration conf,
        Encoder<T> encoder, LocalDate logicalDate, String queryPath) {
      super(name, conf, encoder);
      this.conf = conf;
      this.logicalDate = logicalDate;
      this.queryPath = queryPath;
    }

    @Override
    protected String sql(BatchId batchId) {
      return resourceAsString(queryPath).replace(":hints", conf.getHints());
    }

    @Override
    protected Map<String, Serializable> getBindArguments(BatchId batchId) {
      Builder<String, Serializable> bindArguments = ImmutableMap.<String, Serializable>builder()
          .put("logicalDate", Date.valueOf(logicalDate))
          .put("partitions", String.valueOf(conf.getPartitionHighBound()));

      return bindArguments.build();
    }

    @Override
    protected PartitionParameters getPartitionParameters() {
      return byColumn("partitionId", conf.getPartitionCount(), conf.getPartitionHighBound());
    }
  }

}
