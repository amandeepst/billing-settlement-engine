package com.worldpay.pms.bse.engine.data;

import com.worldpay.pms.bse.domain.model.mincharge.MinimumCharge;
import com.worldpay.pms.bse.domain.model.tax.Party;
import com.worldpay.pms.bse.domain.model.tax.ProductCharacteristic;
import com.worldpay.pms.bse.domain.model.tax.TaxRate;
import com.worldpay.pms.bse.domain.model.tax.TaxRule;
import com.worldpay.pms.bse.domain.staticdata.Currency;

public interface BillingRepository {

  String getCalculationTypeBillDisplay(String calculationLineType);

  Short getRoundingScale(String currency);

  Iterable<Currency> getCurrencies();

  String getProductDescription(String productCode);

  String getCalcLineTypeDescription(String calcLineType);

  String getSvcQtyDescription(String svcQtyCode);

  Iterable<Party> getParties();

  Iterable<TaxRule> getTaxRules();

  Iterable<TaxRate> getTaxRates();

  Iterable<ProductCharacteristic> getProductCharacteristics();

  Iterable<MinimumCharge> getMinimumCharges();
}
