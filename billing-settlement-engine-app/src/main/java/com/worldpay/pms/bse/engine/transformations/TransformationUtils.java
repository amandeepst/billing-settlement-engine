package com.worldpay.pms.bse.engine.transformations;

import static scala.Tuple2.apply;

import com.worldpay.pms.bse.domain.common.Utils;
import io.vavr.collection.Stream;
import java.math.BigDecimal;
import java.sql.Date;
import java.time.Instant;
import java.util.Iterator;
import lombok.experimental.UtilityClass;
import org.apache.spark.TaskContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.hashids.Hashids;
import scala.Tuple2;

@UtilityClass
public class TransformationUtils {

  // character `?` reserved for emergencies
  public static final String ALPHABET = "!$%*+-0123456789:;<=>@ABCDEFGHIJKLMNOPQRSTUVWXYZ^_abcdefghijklmnopqrstuvwxyz~#";

  public static Date getCurrentDate() {
    return new Date(Instant.now().toEpochMilli());
  }

  public static BigDecimal addNullableAmounts(BigDecimal x, BigDecimal y) {
    if (x == null) {
      return y;
    }
    if (y == null) {
      return x;
    }
    return x.add(y);
  }

  public static <T> Dataset<Tuple2<String, T>> enrichDatasetWithUUIds(Dataset<T> ds, Encoder<T> encoding) {
    return ds.mapPartitions(TransformationUtils::enrichWithUUIds, Encoders.tuple(Encoders.STRING(), encoding));
  }

  private static <T> Iterator<Tuple2<String, T>> enrichWithUUIds(Iterator<T> partition) {
    return Stream.ofAll(() -> partition)
        .map(element -> new Tuple2<>(Utils.generateId(), element))
        .iterator();
  }

  public static <T> Dataset<Tuple2<String, T>> enrichDatasetWithIdsOfSpecificLength(Dataset<T> ds, long runId, int idLength,
      Encoder<T> encoding) {
    return ds.mapPartitions(p -> enrichWithIds(runId, idLength, p), Encoders.tuple(Encoders.STRING(), encoding));
  }

  private static <T> Iterator<Tuple2<String, T>> enrichWithIds(long runId, int idLength, Iterator<T> partition) {
    Hashids hashids = new Hashids("", idLength, ALPHABET);
    return Stream.ofAll(() -> partition)
        .zipWithIndex()
        .map(t -> apply(generateId(hashids, runId, TaskContext.getPartitionId(), t._2), t._1))
        .iterator();
  }

  private static String generateId(Hashids hashids, long runId, int partitionId, long idx) {
    return hashids.encode(runId, partitionId, idx);
  }
}
