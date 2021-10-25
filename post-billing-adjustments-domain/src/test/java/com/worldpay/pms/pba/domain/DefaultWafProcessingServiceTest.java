package com.worldpay.pms.pba.domain;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;

import com.worldpay.pms.pba.domain.model.Adjustment;
import com.worldpay.pms.pba.domain.model.Bill;
import com.worldpay.pms.pba.domain.model.BillAdjustment;
import com.worldpay.pms.pba.domain.model.TestBill;
import com.worldpay.pms.pba.domain.model.WithholdFunds;
import io.vavr.collection.List;
import io.vavr.control.Option;
import java.math.BigDecimal;
import java.sql.Date;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;

class DefaultWafProcessingServiceTest {

  public final static Supplier<TestBill.TestBillBuilder> BILL = () -> TestBill.builder()
      .billId("bill1")
      .billNumber("123")
      .billSubAccountId("1234")
      .accountType("FUND")
      .accountId("0215874")
      .businessUnit("WPBU")
      .billDate(Date.valueOf("2021-02-17"))
      .billCycleId("WPDY")
      .startDate(Date.valueOf("2021-02-17"))
      .endDate(Date.valueOf("2021-02-17"))
      .billReference("REF")
      .status("STATUS")
      .adhocBillFlag("N")
      .releaseReserveIndicator("N")
      .releaseWafIndicator("N")
      .fastestPaymentRouteIndicator("N")
      .individualBillIndicator("N")
      .manualNarrative(" ")
      .partyId("P00007")
      .legalCounterparty("P00001")
      .billAmount(BigDecimal.valueOf(-1L))
      .currencyCode("GPB");

  DefaultWafProcessingService service;

  @Test
  void whenNoWithholdThenReturnEmptyList() {
    service = new DefaultWafProcessingService(accountId -> Option.none(), key -> Option.none(),
        accountId -> Option.of(""));

    List<Bill> bills = List.of(BILL.get().build());
    List<BillAdjustment> result = service.computeAdjustment(bills);

    assertThat(result.isEmpty(), is(true));
  }

  @Test
  void whenTargetIsNullThenWithholdAllFunds() {
    service = new DefaultWafProcessingService(
        accountId -> Option.of(new WithholdFunds("2", "WAF", null, null)),
        key -> Option.none(), accountId -> Option.of(""));

    TestBill bill1 = BILL.get().build();
    TestBill bill2 = BILL.get().billId("bill2").billAmount(BigDecimal.valueOf(-2L)).build();
    TestBill bill3 = BILL.get().billId("bill3").billAmount(BigDecimal.TEN).build();
    TestBill bill4 = BILL.get().billId("bill4").billAmount(BigDecimal.valueOf(8L)).build();

    List<Bill> bills = List.of(bill1, bill2, bill3, bill4);
    List<BillAdjustment> result = service.computeAdjustment(bills);

    assertThat(result, hasItem(
        new BillAdjustment(new Adjustment("WAFBLD", "Withhold Funds Build - Full", BigDecimal.valueOf(2D), ""),
            bill2)));
    assertThat(result, hasItem(
        new BillAdjustment(new Adjustment("WAFBLD", "Withhold Funds Build - Full", BigDecimal.valueOf(1D), ""),
            bill1)));
    assertThat(result, hasItem(
        new BillAdjustment(new Adjustment("WAFBLD", "Withhold Funds Build - Full", BigDecimal.valueOf(-8D), ""),
            bill4)));
    assertThat(result, hasItem(
        new BillAdjustment(new Adjustment("WAFBLD", "Withhold Funds Build - Full", BigDecimal.valueOf(-10D), ""),
            bill3)));

  }

  @Test
  void whenTargetIsNullAndPercentNotNullThenWithholdJustTheRightAmount() {
    service = new DefaultWafProcessingService(
        accountId -> Option.of(new WithholdFunds("2", "WAF", null, 80D)),
        key -> Option.none(), accountId -> Option.of(""));

    TestBill bill1 = BILL.get().billId("bill1").billAmount(BigDecimal.valueOf(-100L)).build();
    TestBill bill2 = BILL.get().billId("bill2").billAmount(BigDecimal.valueOf(-120L)).build();
    TestBill bill3 = BILL.get().billId("bill3").billAmount(BigDecimal.valueOf(100L)).build();
    TestBill bill4 = BILL.get().billId("bill4").billAmount(BigDecimal.valueOf(80L)).build();

    List<Bill> bills = List.of(bill1, bill2, bill3, bill4);
    List<BillAdjustment> result = service.computeAdjustment(bills);

    assertThat(result, hasItem(
        new BillAdjustment(new Adjustment("WAFPBLD", "Withhold Funds Build - Partial", BigDecimal.valueOf(96D), ""),
            bill2)));
    assertThat(result, hasItem(
        new BillAdjustment(new Adjustment("WAFPBLD", "Withhold Funds Build - Partial", BigDecimal.valueOf(80D), ""),
            bill1)));
    assertThat(result, hasItem(
        new BillAdjustment(new Adjustment("WAFPBLD", "Withhold Funds Build - Partial", BigDecimal.valueOf(-64D), ""),
            bill4)));
    assertThat(result, hasItem(
        new BillAdjustment(new Adjustment("WAFPBLD", "Withhold Funds Build - Partial", BigDecimal.valueOf(-80D), ""),
            bill3)));
  }

  @Test
  void whenTargetIsNotNullAndPercentIsNullThenWithholdJustTheRightAmountFromTheLargestBills() {
    service = new DefaultWafProcessingService(
        accountId -> Option.of(new WithholdFunds("2", "WAF", BigDecimal.valueOf(140), null)),
        key -> Option.none(), accountId -> Option.of(""));

    TestBill bill1 = BILL.get().billAmount(BigDecimal.valueOf(-100L)).build();
    TestBill bill2 = BILL.get().billAmount(BigDecimal.valueOf(-120L)).build();
    TestBill bill3 = BILL.get().billAmount(BigDecimal.valueOf(-80L)).build();

    List<Bill> bills = List.of(bill1, bill2, bill3);
    List<BillAdjustment> result = service.computeAdjustment(bills);
    List<BillAdjustment> expected = List.of(
        new BillAdjustment(new Adjustment("WAFBLD", "Withhold Funds Build - Full", BigDecimal.valueOf(120D), ""),
            bill2),
        new BillAdjustment(new Adjustment("WAFPBLD", "Withhold Funds Build - Partial", BigDecimal.valueOf(20D), ""),
            bill1));

    assertThat(result, is(expected));
  }

  @Test
  void whenTargetIsNotNullAndPercentIsNullAndDebitThenWithholdJustTheRightAmountFromTheLargestBills() {
    service = new DefaultWafProcessingService(
        accountId -> Option.of(new WithholdFunds("2", "WAF", BigDecimal.valueOf(140), null)),
        key -> Option.none(), accountId -> Option.of(""));

    TestBill bill1 = BILL.get().billId("bill1").billAmount(BigDecimal.valueOf(100L)).build();
    TestBill bill2 = BILL.get().billId("bill2").billAmount(BigDecimal.valueOf(-120L)).build();
    TestBill bill3 = BILL.get().billId("bill3").billAmount(BigDecimal.valueOf(-200L)).build();

    List<Bill> bills = List.of(bill1, bill2, bill3);
    List<BillAdjustment> result = service.computeAdjustment(bills);
    List<BillAdjustment> expected = List.of(
        new BillAdjustment(new Adjustment("WAFBLD", "Withhold Funds Build - Full", BigDecimal.valueOf(-100D), ""),
            bill1),
        new BillAdjustment(new Adjustment("WAFBLD", "Withhold Funds Build - Full", BigDecimal.valueOf(200D), ""),
            bill3),
    new BillAdjustment(new Adjustment("WAFPBLD", "Withhold Funds Build - Partial", BigDecimal.valueOf(40D), ""),
        bill2));

    assertThat(result, is(expected));
  }

  @Test
  void whenTargetIsNotNullAndPercentIsNotNullThenWithholdJustTheRightAmountFromTheLargestBills() {
    service = new DefaultWafProcessingService(
        accountId -> Option.of(new WithholdFunds("2", "WAF", BigDecimal.valueOf(140), 80D)),
        key -> Option.of(BigDecimal.valueOf(-20L)),
        accountId -> Option.of("sa3"));

    TestBill bill1 = BILL.get().billId("bill1").billAmount(BigDecimal.valueOf(-100L)).build();
    TestBill bill2 = BILL.get().billId("bill2").billAmount(BigDecimal.valueOf(-120L)).build();
    TestBill bill3 = BILL.get().billId("bill3").billAmount(BigDecimal.valueOf(100L)).build();
    TestBill bill4 = BILL.get().billId("bill4").billAmount(BigDecimal.valueOf(150L)).build();

    List<Bill> bills = List.of(bill1, bill2, bill3, bill4);
    List<BillAdjustment> result = service.computeAdjustment(bills);
    List<BillAdjustment> expected = List.of(
        new BillAdjustment(new Adjustment("WAFPBLD", "Withhold Funds Build - Partial", BigDecimal.valueOf(-80D), "sa3"),
            bill3),
        new BillAdjustment(new Adjustment("WAFPBLD", "Withhold Funds Build - Partial", BigDecimal.valueOf(-120D), "sa3"),
            bill4),
        new BillAdjustment(new Adjustment("WAFPBLD", "Withhold Funds Build - Partial", BigDecimal.valueOf(96D), "sa3"),
            bill2),
        new BillAdjustment(new Adjustment("WAFPBLD", "Withhold Funds Build - Partial", BigDecimal.valueOf(80D), "sa3"),
            bill1));

    assertThat(result, is(expected));
  }
}