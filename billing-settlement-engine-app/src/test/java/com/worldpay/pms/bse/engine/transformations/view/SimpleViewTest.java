package com.worldpay.pms.bse.engine.transformations.view;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.worldpay.pms.testing.utils.DbUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public abstract class SimpleViewTest extends ViewBaseTest {

  private static final String BATCH_CODE_1 = "B_ID_0001";
  private static final String BATCH_CODE_2 = "B_ID_0002";

  private static final String ID_ENTRY_1 = "1";
  private static final String ID_ENTRY_2 = "2";

  @BeforeEach
  void setUp() {
    DbUtils.cleanUp(sqlDb, getTableName());
  }

  protected abstract String getTableName();

  protected abstract void insertEntry(String id, String batchHistoryCode, int batchAttempt);

  protected abstract long countVwEntryByAllFields(String id);

  void assertVwEntryExists(String id) {
    long count = countVwEntryByAllFields(id);
    assertThat(count, is(1L));
  }

  void assertVwEntryNotExists(String id) {
    long count = countVwEntryByAllFields(id);
    assertThat(count, is(0L));
  }

  @Test
  void whenBatchIsFailedThenOneEntryIsNotReturned() {
    insertBatchHistoryAndOnBatchCompleted(BATCH_CODE_1, 1, STATE_FAILED);
    insertEntry(ID_ENTRY_1, BATCH_CODE_1, 1);

    assertVwEntryNotExists(ID_ENTRY_1);
  }

  @Test
  void whenBatchIsCompletedThenOneEntryIsReturned() {
    insertBatchHistoryAndOnBatchCompleted(BATCH_CODE_1, 1, STATE_COMPLETED);
    insertEntry(ID_ENTRY_1, BATCH_CODE_1, 1);

    assertVwEntryExists(ID_ENTRY_1);
  }

  @Test
  void whenBatchIsCompletedThenMultipleEntriesAreReturned() {
    insertBatchHistoryAndOnBatchCompleted(BATCH_CODE_1, 1, STATE_COMPLETED);
    insertEntry(ID_ENTRY_1, BATCH_CODE_1, 1);
    insertEntry(ID_ENTRY_2, BATCH_CODE_1, 1);

    assertVwEntryExists(ID_ENTRY_1);
    assertVwEntryExists(ID_ENTRY_2);
  }

  @Test
  void whenTwoCompletedBatchesExistThenAllTheirEntriesAreReturned() {
    insertBatchHistoryAndOnBatchCompleted(BATCH_CODE_1, 1, STATE_COMPLETED);
    insertBatchHistoryAndOnBatchCompleted(BATCH_CODE_2, 1, STATE_COMPLETED);
    insertEntry(ID_ENTRY_1, BATCH_CODE_1, 1);
    insertEntry(ID_ENTRY_2, BATCH_CODE_2, 1);

    assertVwEntryExists(ID_ENTRY_1);
    assertVwEntryExists(ID_ENTRY_2);
  }



}
