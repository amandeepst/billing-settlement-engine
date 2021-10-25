package com.worldpay.pms.bse.domain.validation;

import static com.worldpay.pms.utils.Strings.isNotNullOrEmptyOrWhitespace;

import com.worldpay.pms.bse.domain.common.ErrorCatalog;
import com.worldpay.pms.bse.domain.model.BillableItem;
import com.worldpay.pms.pce.common.DomainError;
import io.vavr.collection.List;
import io.vavr.collection.Seq;
import io.vavr.control.Validation;
import java.util.Arrays;

public class DefaultValidationService implements ValidationService {

  @Override
  public Validation<Seq<DomainError>, BillableItem> apply(BillableItem billableItem) {
    return flattenErrors(
        Validation.combine(
            verifyCommonFields(billableItem),
            (billableItem.isStandardBillableItem()) ? verifyStandardFields(billableItem) : verifyMiscFields(billableItem)
        ).ap((a, b) -> billableItem));
  }

  private Validation<Seq<DomainError>, BillableItem> verifyMiscFields(BillableItem billableItem) {
    return flattenErrors(
        Validation.combine(
            Validation.combine(
                validateBillableItemId(billableItem, "misc_bill_item_id"),
                validateReleaseWAFIndicator(billableItem),
                validateReleaseReserveIndicator(billableItem),
                validateFastestSettlementIndicator(billableItem),
                validateIndividualPaymentIndicator(billableItem)
            ).ap((a, b, c, d, e) -> billableItem),
            Validation.combine(
                validatePaymentNarrative(billableItem),
                validateProductClass(billableItem, "char_val"),
                validateProductId(billableItem, "product_id"),
                validateBillingCurrency(billableItem, "currency_cd"),
                validateAmount(billableItem, "amount"),
                validateRateValue(billableItem, "price")
            ).ap((a, b, c, d, e, f) -> billableItem)
        ).ap((a, b) -> billableItem));
  }

  private Validation<Seq<DomainError>, BillableItem> verifyStandardFields(BillableItem billableItem) {
    return Validation.combine(
        validateBillableItemId(billableItem, "billable_item_id"),
        validateProductClass(billableItem, "product_class"),
        validateProductId(billableItem, "child_product"),
        validateBillingCurrency(billableItem, "billing_currency"),
        validateStandardPriceLineId(billableItem),
        validateAmount(billableItem, "precise_charge_amount"),
        validateRateValue(billableItem, "rate")
    ).ap((a, b, c, d, e, f, g) -> billableItem);
  }

  private Validation<Seq<DomainError>, BillableItem> verifyCommonFields(BillableItem billableItem) {
    return flattenErrors(
        Validation.combine(
            Validation.combine(
                validateSubAccountId(billableItem),
                validateLegalCounterparty(billableItem),
                validateAccruedDate(billableItem),
                validateAdhocBillFlag(billableItem),
                validateOverpaymentIndicator(billableItem)
            ).ap((a, b, c, d, e) -> billableItem),
            Validation.combine(
                validateIlmDate(billableItem),
                validateGranularity(billableItem),
                validateRateType(billableItem),
                validateQuantity(billableItem)
            ).ap((a, b, c, d) -> billableItem)
        ).ap((a, b) -> billableItem));
  }

  private Validation<Seq<DomainError>, BillableItem> flattenErrors(Validation<Seq<Seq<DomainError>>, BillableItem> seqItemValidation) {
    return seqItemValidation.mapError(validationSequence -> validationSequence.fold(List.empty(), Seq::appendAll));
  }

  private Validation<DomainError, BillableItem> validateBillableItemId(BillableItem event, String errorMessageField) {
    return validateNotNullOrEmptyField(event, event.getBillableItemId(), errorMessageField);
  }

  private Validation<DomainError, BillableItem> validateSubAccountId(BillableItem event) {
    return validateNotNullOrEmptyField(event, event.getSubAccountId(), "sub_account_id");
  }

  private Validation<DomainError, BillableItem> validateLegalCounterparty(BillableItem event) {
    return validateNotNullOrEmptyField(event, event.getLegalCounterparty(), "lcp");
  }

  private Validation<DomainError, BillableItem> validateStandardPriceLineId(BillableItem event) {
    return validateNotNullOrEmptyField(event, event.getPriceLineId(), "price_asgn_id");
  }

  private Validation<DomainError, BillableItem> validateProductClass(BillableItem event, String errorMessageField) {
    return validateNotNullOrEmptyField(event, event.getProductClass(), errorMessageField);
  }

  private Validation<DomainError, BillableItem> validateProductId(BillableItem event, String errorMessageField) {
    return validateNotNullOrEmptyField(event, event.getProductId(), errorMessageField);
  }

  private Validation<DomainError, BillableItem> validateAdhocBillFlag(BillableItem event) {
    return validateNotNullOrEmptyField(event, event.getAdhocBillFlag(), "adhoc_bill_flg");
  }

  private Validation<DomainError, BillableItem> validateBillingCurrency(BillableItem event, String errorMessageField) {
    return validateNotNullOrEmptyField(event, event.getBillingCurrency(), errorMessageField);
  }

  private Validation<DomainError, BillableItem> validateGranularity(BillableItem event) {
    return validateNotNullOrEmptyField(event, event.getGranularity(), "sett_level_granularity");
  }

  private Validation<DomainError, BillableItem> validateQuantity(BillableItem event) {
    return (event.getBillableItemServiceQuantity() != null &&
            event.getBillableItemServiceQuantity()
                .getServiceQuantities() != null &&
            event.getBillableItemServiceQuantity()
                .getServiceQuantities()
                .get("TXN_VOL") != null) ?
            Validation.valid(event) :
           ErrorCatalog.unexpectedNull("qty").asValidation();
  }

  private Validation<DomainError, BillableItem> validateIndividualPaymentIndicator(BillableItem event) {
    return validateNotNullOrEmptyField(event, event.getIndividualPaymentIndicator(), "ind_payment_flg");
  }

  private Validation<DomainError, BillableItem> validatePaymentNarrative(BillableItem event) {
    return validateNotNullOrEmptyField(event, event.getPaymentNarrative(), "pay_narrative");
  }

  private Validation<DomainError, BillableItem> validateReleaseReserveIndicator(BillableItem event) {
    return validateNotNullOrEmptyField(event, event.getReleaseReserveIndicator(), "rel_reserve_flg");
  }

  private Validation<DomainError, BillableItem> validateFastestSettlementIndicator(BillableItem event) {
    return validateNotNullOrEmptyField(event, event.getFastestSettlementIndicator(), "fastest_payment_flg");
  }

  private Validation<DomainError, BillableItem> validateReleaseWAFIndicator(BillableItem event) {
    return validateNotNullOrEmptyField(event, event.getReleaseWAFIndicator(), "rel_waf_flg");
  }

  private Validation<DomainError, BillableItem> validateOverpaymentIndicator(BillableItem event) {
    return validateNotNullOrEmptyField(event, event.getOverpaymentIndicator(), "overpayment_indicator");
  }

  private Validation<DomainError, BillableItem> validateNotNullOrEmptyField(BillableItem event, String field, String errorMessageField) {
    return isNotNullOrEmptyOrWhitespace(field) ? Validation.valid(event)
                                               : ErrorCatalog.unexpectedNullOrEmpty(errorMessageField).asValidation();
  }

  private Validation<DomainError, BillableItem> validateAccruedDate(BillableItem event) {
    return event.getAccruedDate() != null ? Validation.valid(event)
                                          : ErrorCatalog.unexpectedNull("accrued_dt").asValidation();
  }

  private Validation<DomainError, BillableItem> validateIlmDate(BillableItem event) {
    return event.getIlmDate() != null ? Validation.valid(event)
                                      : ErrorCatalog.unexpectedNull("ilm_dt").asValidation();
  }

  private Validation<DomainError, BillableItem> validateAmount(BillableItem event, String errorMessageField) {
    return (event.getBillableItemLines() != null &&
            Arrays.stream(event.getBillableItemLines())
                  .allMatch(line -> line.getAmount() != null)) ? Validation.valid(event)
           : ErrorCatalog.unexpectedNull(errorMessageField).asValidation();
  }

  private Validation<DomainError, BillableItem> validateRateType(BillableItem event) {
    return (event.getBillableItemLines() != null &&
            Arrays.stream(event.getBillableItemLines())
                  .allMatch(line -> isNotNullOrEmptyOrWhitespace(line.getRateType()))) ? Validation.valid(event)
           : ErrorCatalog.unexpectedNullOrEmpty("rate_type").asValidation();
  }

  private Validation<DomainError, BillableItem> validateRateValue(BillableItem event, String errorMessageField) {
    return (event.getBillableItemLines() != null &&
            Arrays.stream(event.getBillableItemLines())
                  .allMatch(line -> line.getRateValue() != null)) ? Validation.valid(event)
           : ErrorCatalog.unexpectedNull(errorMessageField).asValidation();
  }
}
