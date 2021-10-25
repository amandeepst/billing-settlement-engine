package com.worldpay.pms.bse.domain.tax;

import static com.worldpay.pms.bse.domain.common.ErrorCatalog.noTaxProductCharacteristicFound;
import static com.worldpay.pms.bse.domain.common.ErrorCatalog.noTaxRateFound;

import com.worldpay.pms.bse.domain.model.completebill.CompleteBillLine;
import com.worldpay.pms.bse.domain.model.tax.ProductCharacteristic;
import com.worldpay.pms.bse.domain.model.tax.ProductCharacteristicLookupKey;
import com.worldpay.pms.bse.domain.model.tax.TaxRate;
import com.worldpay.pms.bse.domain.model.tax.TaxRateLookupKey;
import com.worldpay.pms.bse.domain.model.tax.TaxRule;
import com.worldpay.pms.pce.common.DomainError;
import io.vavr.collection.HashMap;
import io.vavr.collection.Map;
import io.vavr.collection.Stream;
import io.vavr.control.Option;
import io.vavr.control.Validation;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.NonNull;

public class DefaultTaxRateDeterminationService implements TaxRateDeterminationService {

  private final Map<ProductCharacteristicLookupKey, ProductCharacteristic> taxProductCharacteristics;
  private final Map<TaxRateLookupKey, TaxRate> taxRates;

  public DefaultTaxRateDeterminationService(Iterable<ProductCharacteristic> taxProductCharacteristics, Iterable<TaxRate> taxRates) {
    this.taxProductCharacteristics = getProductCharacteristicsAsMap(taxProductCharacteristics);
    this.taxRates = getTaxRatesAsMap(taxRates);
  }

  @Override
  public Validation<DomainError, TaxRate> apply(@NonNull CompleteBillLine billLine, @NonNull TaxRule taxRule) {
    return getProductCharacteristic(billLine, taxRule)
        .flatMap(productCharacteristic -> getTaxRate(taxRule, productCharacteristic));
  }

  private Map<ProductCharacteristicLookupKey, ProductCharacteristic> getProductCharacteristicsAsMap(
      Iterable<ProductCharacteristic> productCharacteristics) {
    return HashMap.ofAll(
        Stream.ofAll(productCharacteristics)
            .collect(Collectors.toMap(ProductCharacteristicLookupKey::from, Function.identity(),
                (a, b) -> new ProductCharacteristic(a.getProductCode(), a.getCharacteristicType(), "OUT OF SCOPE-O")))
    );
  }

  private Map<TaxRateLookupKey, TaxRate> getTaxRatesAsMap(Iterable<TaxRate> taxRates) {
    return Stream.ofAll(taxRates)
        .toMap(TaxRateLookupKey::from, Function.identity());
  }

  private Validation<DomainError, ProductCharacteristic> getProductCharacteristic(CompleteBillLine billLine, TaxRule taxRule) {
    ProductCharacteristicLookupKey productCharKey =
        new ProductCharacteristicLookupKey(billLine.getProductIdentifier(), taxRule.getTaxStatusType());
    Option<ProductCharacteristic> productCharacteristic = taxProductCharacteristics.get(productCharKey);
    if (productCharacteristic.isEmpty()) {
      return Validation.invalid(noTaxProductCharacteristicFound(productCharKey.getProductCode(), productCharKey.getCharacteristicType()));
    }
    return Validation.valid(productCharacteristic.get());
  }

  private Validation<DomainError, TaxRate> getTaxRate(TaxRule taxRule, ProductCharacteristic productCharacteristic) {
    TaxRateLookupKey taxRateLookupKey =
        new TaxRateLookupKey(taxRule.getTaxStatusType(), productCharacteristic.getCharacteristicValue());
    Option<TaxRate> taxRate = taxRates.get(taxRateLookupKey);
    if (taxRate.isEmpty()) {
      return Validation.invalid(noTaxRateFound(taxRateLookupKey.getTaxStatusType(), taxRateLookupKey.getTaxStatusValue()));
    }
    return Validation.valid(taxRate.get());
  }
}
