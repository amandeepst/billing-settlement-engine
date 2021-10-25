package com.worldpay.pms.bse.domain.account;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Delegate;

@Getter
@Builder(toBuilder = true)
@EqualsAndHashCode
@AllArgsConstructor
public class BillingAccount {

  /**
   * The id for the billing account (if there is a hierarchy this is the parent account)
   */
  @NonNull
  String accountId;

  /**
   * The party id that corresponds to the billable item (child account)
   */
  String childPartyId;

  /**
   * account type for the billing account (parent)
   */
  @NonNull
  String accountType;

  /**
   * the id of the billing (parent) sub account that has the same type as the account type
   * if billing account type is FUND then we will search the FUND sub account of that account
   */
  @NonNull
  String subAccountId;

  /**
   * the sub account type of the billing (parent) sub account
   */
  @NonNull
  String subAccountType;

  /**
   * The party id that corresponds to the billing account (parent account)
   */
  @NonNull
  String partyId;

  /**
   * The legal counterparty id that corresponds to the billing account (parent account)
   */
  @NonNull
  String legalCounterparty;

  /**
   * The currency that corresponds to the billing account (parent account)
   */
  @NonNull
  String currency;

  /**
   * The business unit that corresponds to the billing account (parent account)
   */
  @NonNull
  String  businessUnit;

  /**
   * The processing group that corresponds to the billing account (parent account)
   */
  String processingGroup;

  /**
   * The billing cycle that corresponds to the billing account (parent account)
   */
  @Delegate
  @Setter
  BillingCycle billingCycle;

}
