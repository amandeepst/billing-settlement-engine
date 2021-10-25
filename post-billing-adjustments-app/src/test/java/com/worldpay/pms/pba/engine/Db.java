package com.worldpay.pms.pba.engine;

import static java.lang.String.format;

import com.worldpay.pms.spark.core.jdbc.SqlDb;
import io.vavr.collection.List;
import org.sql2o.Sql2oQuery;
import org.sql2o.data.Column;

public class Db {

  static final List<String> TABLES = List.of(
      "vw_withhold_funds",
      "vw_sub_acct",
      "vwm_merch_acct_ledger_snapshot",
      "vw_bill",
      "vw_post_bill_adj_accounting",
      "vw_post_bill_adj"
  );

  static void createInputTable(SqlDb sourceDb, SqlDb destDb, String table) {
    List<Column> columns = sourceDb.execQuery(
        "get-columns",
        format("SELECT * FROM %s WHERE 1 = 0", table.toUpperCase()),
        q -> List.ofAll(q.executeAndFetchTable().columns())
    );

    String stmt = format(
        "CREATE TABLE %s (%s)",
        table,
        columns.map(c -> format("%s %s", c.getName(), c.getType())).intersperse(", ").mkString()
    );

    destDb.execQuery(table, stmt, Sql2oQuery::executeUpdate);
  }


}
