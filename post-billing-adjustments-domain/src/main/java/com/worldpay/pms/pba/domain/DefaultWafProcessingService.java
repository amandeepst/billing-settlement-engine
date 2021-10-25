package com.worldpay.pms.pba.domain;

import com.worldpay.pms.pba.domain.model.Adjustment;
import com.worldpay.pms.pba.domain.model.Bill;
import com.worldpay.pms.pba.domain.model.BillAdjustment;
import com.worldpay.pms.pba.domain.model.WithholdFunds;
import com.worldpay.pms.pba.domain.model.WithholdFundsBalance.WithholdFundsBalanceKey;
import io.vavr.collection.List;
import io.vavr.control.Option;
import java.math.BigDecimal;

public class DefaultWafProcessingService implements WafProcessingService {

  public static final String DEFAULT_SA_ID = "N/A";

  private final WithholdFundsStore withholdFundsStore;
  private final WithholdFundsBalanceStore withholdFundsBalanceStore;
  private final SubAccountStore subAccountStore;

  public DefaultWafProcessingService(WithholdFundsStore withholdFundsStore,
      WithholdFundsBalanceStore balanceStore,
      SubAccountStore subAccountStore) {
    this.withholdFundsStore = withholdFundsStore;
    this.withholdFundsBalanceStore = balanceStore;
    this.subAccountStore = subAccountStore;
  }

  @Override
  public List<BillAdjustment> computeAdjustment(List<Bill> bills) {
    Bill firstBill = bills.get(0);
    Option<WithholdFunds> withholdFundsOption =
        withholdFundsStore.getWithholdFundForAccount(firstBill.getAccountId());

    BigDecimal initialBalance = withholdFundsBalanceStore
        .get(new WithholdFundsBalanceKey(firstBill.getPartyId(), firstBill.getLegalCounterparty(), firstBill.getCurrencyCode()))
        .getOrElse(BigDecimal.ZERO).negate();

    return withholdFundsOption.map(wf -> getAdjustmentForWithholdFunds(bills, wf, initialBalance))
        .getOrElse(List.empty());
  }

  private List<BillAdjustment> getAdjustmentForWithholdFunds(List<Bill> bills,
      WithholdFunds withholdFunds, BigDecimal initialBalance) {

    BigDecimal target = withholdFunds.getWithholdFundTarget();
    Double percentage = getPercentage(withholdFunds);

    if(target != null) {
      return getBillAdjustmentsForTargetAmount(bills, initialBalance, target, percentage);
    } else {
      return bills.map(bill -> new BillAdjustment(getAdjustmentForBill(bill, percentage, null), bill));
    }
  }

  private double getPercentage(WithholdFunds withholdFunds) {
    return (withholdFunds.getWithholdFundPercentage() == null ? 100
        : withholdFunds.getWithholdFundPercentage()) / 100;
  }

  private List<BillAdjustment> getBillAdjustmentsForTargetAmount(List<Bill> bills, BigDecimal initialBalance,
      BigDecimal target, Double percentage) {

    List<Bill> debitBills = bills.filter(bill -> bill.getBillAmount().compareTo(BigDecimal.ZERO) > 0).sortBy(Bill::getBillAmount);
    List<Bill> creditBills = bills.filter(bill -> bill.getBillAmount().compareTo(BigDecimal.ZERO) < 0).sortBy(Bill::getBillAmount);

    List<Bill>orderedBills = debitBills.appendAll(creditBills);

    List<BillAdjustment> result = List.empty();
    BigDecimal currentBalance = initialBalance;
    for(Bill bill : orderedBills) {
      BigDecimal toWithhold = target.subtract(currentBalance);
      if(toWithhold.compareTo(BigDecimal.ZERO) > 0 || bill.getBillAmount().compareTo(BigDecimal.ZERO) > 0) {
        Adjustment adjustment = getAdjustmentForBill(bill, percentage, toWithhold);
        currentBalance = currentBalance.add(adjustment.getAmount());
        result = result.append(new BillAdjustment(adjustment, bill));
      }
    }
    return result;
  }

  private Adjustment getAdjustmentForBill(Bill bill, Double percentage, BigDecimal toWithhold) {
    BigDecimal withholdAmount = getWithholdAmount(bill.getBillAmount(), percentage, toWithhold);
    String subAccountId = subAccountStore.getWafSubAccountId(bill.getAccountId()).getOrElse(DEFAULT_SA_ID);
    if (withholdAmount.add(bill.getBillAmount()).compareTo(BigDecimal.ZERO) == 0) {
      return new Adjustment("WAFBLD", "Withhold Funds Build - Full", withholdAmount, subAccountId);
    } else {
      return new Adjustment("WAFPBLD", "Withhold Funds Build - Partial", withholdAmount, subAccountId);
    }
  }

  private BigDecimal getWithholdAmount(BigDecimal billAmount, Double percentage, BigDecimal toWithhold) {
    BigDecimal withholdAmount =  billAmount.negate().multiply(BigDecimal.valueOf(percentage));
    if (toWithhold != null && toWithhold.compareTo(withholdAmount) < 0) {
      withholdAmount = toWithhold;
    }
    return withholdAmount;
  }
}



