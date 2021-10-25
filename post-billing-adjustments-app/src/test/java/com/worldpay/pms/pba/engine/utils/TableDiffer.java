package com.worldpay.pms.pba.engine.utils;

import static io.vavr.control.Option.some;
import static java.lang.String.format;

import com.google.common.base.Objects;
import com.worldpay.pms.utils.Strings;
import io.vavr.Tuple;
import io.vavr.collection.Array;
import io.vavr.collection.HashMap;
import io.vavr.collection.HashSet;
import io.vavr.collection.List;
import io.vavr.collection.Map;
import io.vavr.collection.Seq;
import io.vavr.collection.Set;
import io.vavr.control.Option;
import io.vavr.control.Try;
import java.math.BigDecimal;
import lombok.SneakyThrows;
import lombok.experimental.UtilityClass;
import oracle.sql.TIMESTAMP;
import org.sql2o.data.Column;
import org.sql2o.data.Row;
import org.sql2o.data.Table;

@UtilityClass
public class TableDiffer {

  /** columns to be ignored in comparison globally */
  static final Set<String> IGNORE_COLUMNS = HashSet.of("ilm_dt");

  public static Option<String> dataMatches(Table actual, Table expected, Array<String> ids, Set<String> columnsToIgnore) {
    return combine(
      compareTable(actual, expected, ids, columnsToIgnore, "actual"),
      compareTable(expected, actual, ids, columnsToIgnore, "expected")
    );
  }

  private static Option<String> compareTable(Table actual, Table expected, Array<String> ids, Set<String> columnsToIgnore, String label) {
    Map<Array<Object>, Row> actualRows = List.ofAll(actual.rows())
        .toMap(row -> Tuple.of(extractId(row, ids), row));

    return Option.sequence(
        List.ofAll(expected.rows())
            .map(row -> {
              Array<Object> id = extractId(row, ids);
              return actualRows.get(id)
                  .flatMap(actualRow -> compareRow(id, actualRow, row, IGNORE_COLUMNS.union(columnsToIgnore)))
                  .orElse(some(format("Row with id %s not found on " + label , id.mkString(", "))));
            })
            .filter(Option::isDefined)
    ).map(errors -> errors.filter(Strings::isNotNullOrEmptyOrWhitespace).mkString("\n"));
  }

  private static Option<String> compareRow(Array<Object> id, Row actual, Row expected, Set<String> columnsToIgnore) {
    return Option.sequence(
        List.ofAll(expected.asMap().entrySet())
            .filter(e -> !columnsToIgnore.contains(e.getKey()))
            .map(entry -> compareValue(id, entry.getKey(), actual, entry.getValue()))
            .filter(Option::isDefined)
    )
    .map(errors -> errors.filter(Strings::isNotNullOrEmptyOrWhitespace).mkString("\n"));
  }

  private static Option<String> compareValue(Array<Object> id, String column, Row actual, Object expected) {
    return Try.of(() -> compareValue(id, column, actual.getObject(column), expected))
        .getOrElseGet(e -> some(format("Value mismatch on row `%s`. %s", id.mkString(", "), e.getMessage())));
  }

  @SneakyThrows
  private static Option<String> compareValue(Array<Object> id, String column, Object actual, Object expected) {
    if (compareValue(actual, expected))
      return Option.none();

    return some(format(
        "Value mismatch on row `%s`. Column `%s` was `%s` but expected `%s`",
        id.mkString(", "),
        column,
        actual,
        expected
    ));
  }

  @SneakyThrows
  private static boolean compareValue(Object actual, Object expected) {
    if (Objects.equal(actual, expected))
      return true;

    // compare ignoring whitespaces
    if ((actual instanceof String) && (expected instanceof String)) {
      if (((String) actual).trim().equals(((String) expected).trim()))
        return true;
    }

    if ((actual instanceof BigDecimal) && (expected instanceof BigDecimal)) {
      BigDecimal a = (BigDecimal) actual;
      BigDecimal e = (BigDecimal) expected;
      return a.subtract(e).abs().doubleValue() <= 0.0005;
    }

    if ((actual instanceof TIMESTAMP)) {
      if (Objects.equal(((TIMESTAMP) actual).timestampValue(), expected))
        return true;
    }
    return false;
  }

    private static Array<Object> extractId(Row row, Array<String> ids) {
    return ids.map(row::getObject).map(o -> {
      if (o instanceof String)
        return ((String) o).trim();

      return o;
    });
  }

  public static Option<String> rowCountMatch(Table actual, Table expected) {
    return actual.rows().size() == expected.rows().size()
        ? Option.none()
        : some(format("Row count mismatch. Expected %s but found %s on actual.", expected.rows().size(), actual.rows().size()));
  }

  public static Option<String> schemaMatches(Table actual, Table expected) {
    List<Column> actualColumns = List.ofAll(actual.columns());
    List<Column> expectedColumns = List.ofAll(expected.columns());
    if (actual.columns().size() != expected.columns().size()) {
      Set<String> exp = expectedColumns.map(Column::getName).toSet();
      Set<String> act = actualColumns.map(Column::getName).toSet();
      return some(format("Schema Mismatch:\nmissing: %s\nnot-expected: %s",
        exp.diff(act).mkString(", "),
        act.diff(exp).mkString(", ")
      ));
    }


    List<String> missing = expectedColumns
        .filter(e -> actualColumns.find(a -> a.getName().equals(e.getName())).isEmpty())
        .map(Column::getName);

    if (missing.nonEmpty()) {
      return some(format("Schema Mismatch: Could not find these columns in actual: %s ", missing.mkString(", ")));
    }

    return Option.none();
  }

  public static Option<String> columnsMatch(Table actual, String columnA, String columnB) {
    return combine(
        List.ofAll(actual.rows())
        .map(row -> {
            Object a = row.getObject(columnA);
            Object b = row.getObject(columnB);
            return compareValue(a, b)
              ? Option.none()
              : some(
                  format("Expected values of column `%s` and `%s` to be the same but they were different %s != %s for row %s",
                    columnA,
                    columnB,
                    a,
                    b,
                    HashMap.ofAll(row.asMap()).mkString(",")
                  )
            );
          }
        )
    );
  }

  public static Option<String> combine(Option<String>... errors) {
    return combine(Array.of(errors));
  }

  public static Option<String> combine(Seq<Option<String>> errors) {
    return combine(flatten(errors));
  }

  public static Option<String> combine(Option<Seq<String>> errors) {
    return errors.map(error -> error.mkString("\n")).filter(Strings::isNotNullOrEmptyOrWhitespace);
  }

  public static Option<Seq<String>> flatten(Seq<Option<String>> in) {
    if (in.isEmpty())
      return Option.none();

    return some(
        in.filter(Option::isDefined)
          .map(Option::get)
          .filter(Strings::isNotNullOrEmptyOrWhitespace)
    );
  }
}
