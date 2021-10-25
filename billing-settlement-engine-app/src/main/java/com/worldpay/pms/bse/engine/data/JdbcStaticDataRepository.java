package com.worldpay.pms.bse.engine.data;

import static com.worldpay.pms.spark.core.Resource.resourceAsString;
import static java.lang.String.format;

import com.worldpay.pms.bse.domain.account.BillingCycle;
import com.worldpay.pms.bse.domain.model.mincharge.MinimumCharge;
import com.worldpay.pms.bse.domain.model.tax.ProductCharacteristic;
import com.worldpay.pms.bse.domain.staticdata.CalculationLineTypeDescription;
import com.worldpay.pms.bse.domain.staticdata.Currency;
import com.worldpay.pms.bse.domain.staticdata.ProductDescription;
import com.worldpay.pms.bse.domain.staticdata.ServiceQuantityDescription;
import com.worldpay.pms.spark.core.jdbc.JdbcConfiguration;
import com.worldpay.pms.spark.core.jdbc.repositories.Sql2oRepository;
import java.sql.Date;

public class JdbcStaticDataRepository extends Sql2oRepository {

  private static final String ROOT_PATH = "sql/static-data/%s.sql";
  private Date logicalDate;

  public JdbcStaticDataRepository(JdbcConfiguration conf, Date logicalDate) {
    super(conf);
    this.logicalDate = logicalDate;
  }

  public Iterable<ProductCharacteristic> getProductCharacteristics() {
    String name = "get-product-characteristic";
    return db.execQuery(
        name,
        resourceAsString(format(ROOT_PATH, name)),
        q -> q.executeAndFetch(handler(rs -> new ProductCharacteristic(
            ObjectPool.intern(rs.getAndTrimString("productCode")),
            ObjectPool.intern(rs.getAndTrimString("characteristicType")),
            ObjectPool.intern(rs.getAndTrimString("characteristicValue"))
        )))
    );
  }

  public Iterable<Currency> getDatabaseCurrencies() {
    String name = "get-currencies";
    return db.execQuery(
        name,
        resourceAsString(format(ROOT_PATH, name)),
        q -> q.executeAndFetch(handler(rs -> new Currency(
            ObjectPool.intern(rs.getAndTrimString("currencyCode")),
            rs.getShort("roundingScale")
        )))
    );
  }

  public Iterable<ProductDescription> getDatabaseProductDescription() {
    String name = "get-product-description";
    return db.execQuery(
        name,
        resourceAsString(format(ROOT_PATH, name)),
        q -> q.executeAndFetch(handler(rs -> new ProductDescription(
            ObjectPool.intern(rs.getAndTrimString("productCode")),
            ObjectPool.intern(rs.getAndTrimString("description"))
        )))
    );
  }

  public Iterable<ServiceQuantityDescription> getDatabaseServiceQuantityDescription() {
    String name = "get-svc-qty-description";
    return db.execQuery(
        name,
        resourceAsString(format(ROOT_PATH, name)),
        q -> q.executeAndFetch(handler(rs -> new ServiceQuantityDescription(
            ObjectPool.intern(rs.getAndTrimString("serviceQuantityCode")),
            ObjectPool.intern(rs.getAndTrimString("description"))
        )))
    );
  }

  public Iterable<CalculationLineTypeDescription> getDatabaseCalculationLineTypeDescriptions() {
    String name = "get-calc-line-type-description";
    return db.execQuery(
        name,
        resourceAsString(format(ROOT_PATH, name)),
        q -> q.executeAndFetch(handler(rs -> new CalculationLineTypeDescription(
            ObjectPool.intern(rs.getAndTrimString("calculationLineType")),
            ObjectPool.intern(rs.getAndTrimString("description"))
        )))
    );
  }

  public Iterable<MinimumCharge> getMinimumCharge() {
    String name = "get-minimum-charge";
    return db.execQuery(
        name,
        resourceAsString(format(ROOT_PATH, name)),
        q -> q.addParameter("logicalDate", logicalDate)
            .executeAndFetch(handler(rs -> new MinimumCharge(
                ObjectPool.intern(rs.getAndTrimString("priceAssignId")),
                ObjectPool.intern(rs.getAndTrimString("legalCounterparty")),
                ObjectPool.intern(rs.getAndTrimString("partyId")),
                ObjectPool.intern(rs.getAndTrimString("rateType")),
                ObjectPool.intern(rs.getBigDecimal("rate")),
                ObjectPool.intern(rs.getAndTrimString("currency")),
                new BillingCycle(ObjectPool.intern(rs.getAndTrimString("minChargePeriod")), null, null)
            )))
    );
  }
}
