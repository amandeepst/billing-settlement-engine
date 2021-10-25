package com.worldpay.pms.pba.engine;

import static com.google.common.collect.Iterators.transform;
import static org.apache.spark.sql.functions.col;
import static scala.Tuple2.apply;

import com.google.common.collect.Iterators;
import com.worldpay.pms.pba.domain.WafProcessingService;
import com.worldpay.pms.pba.domain.model.BillAdjustment;
import com.worldpay.pms.pba.engine.model.input.BillRow;
import com.worldpay.pms.pba.engine.model.output.BillAdjustmentAccountingRow;
import com.worldpay.pms.spark.core.DataSource;
import com.worldpay.pms.spark.core.DataWriter;
import com.worldpay.pms.spark.core.JavaDriver;
import com.worldpay.pms.spark.core.batch.Batch;
import com.worldpay.pms.spark.core.batch.Batch.BatchId;
import com.worldpay.pms.spark.core.factory.ExecutorLocalFactory;
import com.worldpay.pms.spark.core.factory.Factory;
import io.vavr.collection.List;
import io.vavr.collection.Stream;
import java.util.Iterator;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.spark.TaskContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.hashids.Hashids;
import scala.Tuple2;

@Slf4j
public class Driver extends JavaDriver<PostBillingBatchRunResult> {

  // character `?` reserved for emergencies
  private static final String ALPHABET = "!$%*+-0123456789:;<=>@ABCDEFGHIJKLMNOPQRSTUVWXYZ^_abcdefghijklmnopqrstuvwxyz~#";

  private static final Hashids hashids = new Hashids("", 12, ALPHABET);

  private final DataSource<BillRow> billSource;
  private final DataWriter<Tuple2<String, BillAdjustment>> adjustmentWriter;
  private final DataWriter<BillAdjustmentAccountingRow> adjustmentAccountingWriter;
  private final Function<BatchId, Long> runIdGenerator;

  private final Factory<WafProcessingService> processingServiceFactory;

  public Driver(SparkSession spark,
      Function<BatchId, Long> runIdProvider,
      DataSource<BillRow> billSource,
      DataWriter<Tuple2<String, BillAdjustment>> adjustmentWriter,
      DataWriter<BillAdjustmentAccountingRow> adjustmentAccountingWriter,
      Factory<WafProcessingService> processingServiceFactory) {

    super(spark);
    this.runIdGenerator = runIdProvider;
    this.billSource = billSource;
    this.adjustmentWriter = adjustmentWriter;
    this.adjustmentAccountingWriter = adjustmentAccountingWriter;
    this.processingServiceFactory = processingServiceFactory;
  }

  public PostBillingBatchRunResult run(Batch batch) {

    log.info(
        "Running batch={} attempt={} from={} to={} ilm_dt={}",
        batch.id.code,
        batch.id.attempt,
        batch.id.watermark.low,
        batch.id.watermark.high,
        batch.createdAt
    );

    long runId = runIdGenerator.apply(batch.id);
    log.info("Generated runId `{}` for batch `{}`", runId, batch.id);

    val wafProcessingFactoryBroadcast = ExecutorLocalFactory.of(jsc, processingServiceFactory);

    Dataset<BillRow> withholdFundsBills = billSource.load(spark, batch.id)
        .filter(col("accountType").equalTo("FUND").and(col("releaseWAFIndicator").equalTo("N")))
        .persist(StorageLevel.MEMORY_AND_DISK_SER());

    val groupedBills = withholdFundsBills.groupByKey(BillRow::getAccountId,  Encoders.STRING())
        .mapGroups((k, iter) -> Iterators.toArray(iter, BillRow.class), Encodings.BILL_ROW_ARRAY_ENCODER);

    Dataset<BillAdjustment> postBillAdjustments = toBillAdjustmentResult(groupedBills, wafProcessingFactoryBroadcast)
        .persist(StorageLevel.MEMORY_AND_DISK_SER());

    Dataset<BillAdjustmentAccountingRow> billAdjustmentsAccounting = generatePostBillAdjustmentsAccounting(postBillAdjustments, runId);

    Dataset<Tuple2<String, BillAdjustment>> postBillAdjustmentsWithIds = generatePostBillAdjustmentsWithIds(
        postBillAdjustments, runId);

    return PostBillingBatchRunResult.builder()
        .billCount(withholdFundsBills.count())
        .adjustmentCount(writeAdjustments(postBillAdjustmentsWithIds, batch))
        .adjustmentAccountingCount(writeAdjustmentsAccounting(billAdjustmentsAccounting, batch))
        .build();
  }

  private static Dataset<BillAdjustment> toBillAdjustmentResult(Dataset<BillRow[]> billRowsDataset,
      Broadcast<Factory<WafProcessingService>> wafProcessingFactoryBroadcast) {
    return billRowsDataset.mapPartitions(
        p -> toBillAdjustment(p, wafProcessingFactoryBroadcast.value().build().get()), Encodings.BILL_ADJUSTMENT_ENCODER);
  }

  private static Iterator<BillAdjustment> toBillAdjustment(Iterator<BillRow[]> rows,
      WafProcessingService wafProcessingService) {
    Iterator<List<BillAdjustment>> results = transform(rows, row -> getAdjustmentsForAccount(row, wafProcessingService));
    return io.vavr.collection.Iterator.ofAll(results).flatMap(Function.identity());
  }

  private static List<BillAdjustment> getAdjustmentsForAccount(BillRow[] bills,
      WafProcessingService wafProcessingService){
    return wafProcessingService.computeAdjustment(List.of(bills));
  }

  private static Dataset<BillAdjustmentAccountingRow> generatePostBillAdjustmentsAccounting(Dataset<BillAdjustment> postBillAdjustments,
      long runId) {
    return postBillAdjustments
        .mapPartitions(p -> generateBillAdjustmentAccounting(runId, p), Encodings.BILL_ADJUSTMENT_ACCOUNTING_ENCODER);
  }

  private static Iterator<BillAdjustmentAccountingRow> generateBillAdjustmentAccounting(long runId, Iterator<BillAdjustment> partition){
    return Stream.ofAll(() -> partition)
        .zipWithIndex()
        .map(t2 -> new Tuple2<BillAdjustment, Integer>(t2._1, t2._2 * 2))
        .flatMap(t2 -> List.of(
            BillAdjustmentAccountingRow.billRowFromAdjustment(t2._1, generateBillAdjustmentId(runId, TaskContext.getPartitionId(),t2._2)),
            BillAdjustmentAccountingRow.wafRowFromAdjustment(t2._1, generateBillAdjustmentId(runId, TaskContext.getPartitionId(),t2._2+1))))
        .iterator();
  }

  private static Dataset<Tuple2<String, BillAdjustment>> generatePostBillAdjustmentsWithIds(Dataset<BillAdjustment> postBillAdjustments,
      long runId) {
    return postBillAdjustments
        .mapPartitions(p -> generateBillAdjustmentId(runId, p), Encodings.BILL_ADJUSTMENT_WITH_ID_ENCODER);
  }

  private static Iterator<Tuple2<String, BillAdjustment>> generateBillAdjustmentId(long runId, Iterator<BillAdjustment> partition) {
    return Stream.ofAll(() -> partition)
        .zipWithIndex()
        .map(t -> apply(generateBillAdjustmentId(runId, TaskContext.getPartitionId(), t._2), t._1))
        .iterator();
  }

  private static String generateBillAdjustmentId(long runId, int partitionId, long idx) {
    return hashids.encode(
        runId,
        partitionId,
        idx
    );
  }

  private long writeAdjustments(Dataset<Tuple2<String, BillAdjustment>> postBillAdjustments, Batch batch) {
    return namedAction("Writing Post Bill Adjustments", sparkContext ->
        adjustmentWriter.write(batch.id, batch.createdAt, postBillAdjustments)
    );
  }

  private long writeAdjustmentsAccounting(Dataset<BillAdjustmentAccountingRow> postBillAdjustmentsAccounting, Batch batch) {
    return namedAction("Writing Post Bill Adjustments Accounting", sparkContext ->
        adjustmentAccountingWriter.write(batch.id, batch.createdAt, postBillAdjustmentsAccounting)
    );
  }

}