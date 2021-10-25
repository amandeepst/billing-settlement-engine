package com.worldpay.pms.bse.engine.data;

import static com.worldpay.pms.bse.engine.data.DataUtils.getDateOrDefault;
import static com.worldpay.pms.bse.engine.data.DataUtils.getStaticDataCsvRecords;

import com.univocity.parsers.common.record.Record;
import com.worldpay.pms.bse.domain.model.tax.Country;
import com.worldpay.pms.bse.domain.model.tax.TaxRate;
import com.worldpay.pms.bse.domain.model.tax.TaxRule;
import com.worldpay.pms.bse.engine.data.billdisplay.CalculationTypeBillDisplay;
import io.vavr.collection.Map;
import java.sql.Date;
import lombok.Getter;

public class CsvStaticDataRepository {

  private final Date logicalDate;

  @Getter
  private final Map<String, String> calculationTypeBillDisplays;

  public CsvStaticDataRepository(Date logicalDate) {
    this.logicalDate = logicalDate;
    this.calculationTypeBillDisplays = buildCalcTypeBillDisplayRepo();
  }

  public Iterable<Country> getCountries() {
    return getStaticDataCsvRecords("tax/country.csv")
        .filter(record -> isDateValid(record, logicalDate, "VALID_FROM", "VALID_TO"))
        .map(record -> new Country(
            ObjectPool.intern(record.getString("COUNTRY_ID")),
            ObjectPool.intern(record.getString("CURRENCY_ID")),
            ObjectPool.intern(record.getString("NAME")),
            ObjectPool.intern(record.getString("EU_GEOGRAPHIC_CODE")),
            ObjectPool.intern(record.getString("STATE_BASED_TAX")),
            ObjectPool.intern(record.getString("EEA_FLG"))
        ));
  }

  public Iterable<TaxRule> getTaxRules() {
    return getStaticDataCsvRecords("tax/tax-rules.csv")
        .filter(record -> isDateValid(record, logicalDate))
        .map(record -> new TaxRule(
            ObjectPool.intern(record.getString("LCP")),
            ObjectPool.intern(record.getString("LCP Country (supplier)")),
            ObjectPool.intern(record.getString("LCP Currency")),
            ObjectPool.intern(record.getString("Merchant Country")),
            ObjectPool.intern(record.getString("Merchant Region")),
            ObjectPool.intern(record.getString("Merchant Tax Reg'd")),
            ObjectPool.intern(record.getString("Tax Authority")),
            ObjectPool.intern(record.getString("Tax Type")),
            ObjectPool.intern(record.getString("Tax Status")),
            ObjectPool.intern(record.getString("Reverse Charge")),
            ObjectPool.intern(record.getString("LCP Tax Reg Number"))
        ));
  }

  public Iterable<TaxRate> getTaxRates() {
    return getStaticDataCsvRecords("tax/tax-rates.csv")
        .filter(record -> isDateValid(record, logicalDate))
        .map(record -> new TaxRate(
            ObjectPool.intern(record.getString("Tax Status Type")),
            ObjectPool.intern(record.getString("Tax Status Value")),
            ObjectPool.intern(record.getString("Tax Status Code")),
            ObjectPool.intern(record.getString("Tax Status Description")),
            ObjectPool.intern(record.getBigDecimal("Tax Rate"))
        ));
  }

  private Map<String, String> buildCalcTypeBillDisplayRepo() {
    return getStaticDataCsvRecords("calculation-type-bill-display.csv")
        .filter(record -> isDateValid(record, logicalDate))
        .map(record -> new CalculationTypeBillDisplay(
            ObjectPool.intern(record.getString("Calculation Line Type")),
            ObjectPool.intern(record.getString("Include on Bill Output"))))
        .toMap(CalculationTypeBillDisplay::getCalculationLineType, CalculationTypeBillDisplay::getIncludeOnBillOutput);
  }

  private boolean isDateValid(Record record, Date logicalDate) {
    return isDateValid(record, logicalDate, "Valid From", "Valid To");
  }

  private boolean isDateValid(Record record, Date logicalDate, String validFromColumnName, String validToColumnName) {
    Date validFrom = getDateOrDefault(record.getString(validFromColumnName), "2020-01-01");
    Date validTo = getDateOrDefault(record.getString(validToColumnName), "9999-12-31");

    return logicalDate.after(validFrom) && logicalDate.before(validTo);
  }
}
