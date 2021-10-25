package com.worldpay.pms.bse.engine;

import com.worldpay.pms.bse.domain.model.mincharge.MinimumCharge;
import com.worldpay.pms.bse.domain.model.tax.Party;
import com.worldpay.pms.bse.domain.model.tax.ProductCharacteristic;
import com.worldpay.pms.bse.domain.model.tax.TaxRate;
import com.worldpay.pms.bse.domain.model.tax.TaxRule;
import com.worldpay.pms.bse.domain.staticdata.Currency;
import com.worldpay.pms.bse.engine.data.BillingRepository;
import com.worldpay.pms.bse.engine.data.billdisplay.CalculationTypeBillDisplay;
import java.sql.Date;
import java.util.Collection;
import lombok.Builder;
import lombok.Singular;
import lombok.Value;

@Value
@Builder
public class InMemoryBillingRepository implements BillingRepository {

  @Singular("addCalculationTypeBillDisplay")
  Collection<CalculationTypeBillDisplay> calculationTypeBillDisplays;
  @Singular("addParty")
  Collection<Party> parties;
  @Singular("addTaxRate")
  Collection<TaxRate> taxRates;
  @Singular("addTaxRule")
  Collection<TaxRule> taxRules;
  @Singular("addProductCharacteristic")
  Collection<ProductCharacteristic> productCharacteristics;
  @Singular("addCurrency")
  Collection<Currency> currencies;
  @Singular("addMinimumCharge")
  Collection<MinimumCharge> minimumCharges;

  Date logicalDate;

  @Override
  public String getCalculationTypeBillDisplay(String calculationLineType) {
    return calculationTypeBillDisplays.stream()
        .filter(c -> c.getCalculationLineType().equals(calculationLineType))
        .map(CalculationTypeBillDisplay::getIncludeOnBillOutput)
        .findAny()
        .orElse("N");
  }

  @Override
  public Iterable<Currency> getCurrencies() {
    return currencies;
  }

  @Override
  public Short getRoundingScale(String currency) {
    return (short) 2;
  }

  @Override
  public String getProductDescription(String productCode) {
    return "productDescription";
  }

  @Override
  public String getCalcLineTypeDescription(String key) {
    return null;
  }

  @Override
  public String getSvcQtyDescription(String svcQtyCode) {
    return null;
  }

  @Override
  public Iterable<MinimumCharge> getMinimumCharges() {
    return minimumCharges;
  }
}
