package org.infinispan.tx.gmu.ssi;

import javax.transaction.Transaction;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

import static junit.framework.Assert.assertEquals;
import static org.infinispan.transaction.gmu.manager.SortedTransactionQueue.TransactionEntry;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
@Test(groups = "functional", testName = "tx.gmu.ssi.DistConsistencyTest")
public class DistConsistencyTest extends AbstractSSITest {

   public void testGetSnapshot() throws Exception {
      assertAtLeastCaches(2);
      rewireMagicKeyAwareConsistentHash();

      Object cache0Key0 = newKey(0, 1);
      Object cache1Key1 = newKey(1, 0);
      Object cache1Key2 = newKey(1, 0);

      logKeysUsedInTest("testGetSnapshot", cache0Key0, cache1Key1, cache1Key2);

      assertKeyOwners(cache0Key0, 0, 1);
      assertKeyOwners(cache1Key1, 1, 0);
      assertKeyOwners(cache1Key2, 1, 0);
      assertCacheValuesNull(cache0Key0, cache1Key1, cache1Key2);

      tm(0).begin();
      cache(0).markAsWriteTransaction();
      txPut(0, cache0Key0, VALUE_1, null);
      txPut(0, cache1Key1, VALUE_1, null);
      txPut(0, cache1Key2, VALUE_1, null);
      tm(0).commit();

      assertCachesValue(0, cache0Key0, VALUE_1);
      assertCachesValue(0, cache1Key1, VALUE_1);
      assertCachesValue(0, cache1Key2, VALUE_1);

      tm(0).begin();
      assert VALUE_1.equals(cache(0).get(cache0Key0));
      Transaction tx0 = tm(0).suspend();

      tm(1).begin();
      cache(1).markAsWriteTransaction();
      cache(1).put(cache0Key0, VALUE_2);
      cache(1).put(cache1Key1, VALUE_2);
      cache(1).put(cache1Key2, VALUE_2);
      tm(1).commit();

      assertCachesValue(1, cache0Key0, VALUE_2);
      assertCachesValue(1, cache1Key1, VALUE_2);
      assertCachesValue(1, cache1Key2, VALUE_2);

      tm(0).resume(tx0);
      assert VALUE_1.equals(cache(0).get(cache0Key0));
      assert VALUE_1.equals(cache(0).get(cache1Key1));
      assert VALUE_1.equals(cache(0).get(cache1Key2));
      tm(0).commit();

      assertNoTransactions();
      printDataContainer();
   }

   public void testPrematureAbort() throws Exception {
      assertAtLeastCaches(2);
      rewireMagicKeyAwareConsistentHash();

      Object cache0Key = newKey(0, 1);
      Object cache1Key = newKey(1, 0);

      logKeysUsedInTest("testPrematureAbort", cache0Key, cache1Key);

      assertKeyOwners(cache0Key, 0, 1);
      assertKeyOwners(cache1Key, 1, 0);
      assertCacheValuesNull(cache0Key, cache1Key);

      tm(0).begin();
      cache(0).markAsWriteTransaction();
      txPut(0, cache0Key, VALUE_1, null);
      txPut(0, cache1Key, VALUE_1, null);
      tm(0).commit();


      tm(0).begin();
      cache(0).markAsWriteTransaction();
      Object value = cache(0).get(cache0Key);
      assertEquals(VALUE_1, value);
      Transaction tx0 = tm(0).suspend();

      tm(1).begin();
      cache(1).markAsWriteTransaction();
      txPut(1, cache0Key, VALUE_2, VALUE_1);
      txPut(1, cache1Key, VALUE_2, VALUE_1);
      tm(1).commit();

      tm(0).resume(tx0);
      try{
         txPut(0, cache0Key, VALUE_3, VALUE_1);
         cache(0).get(cache1Key);
         tm(0).commit();
         assert false : "Expected to abort conflicting transaction";
      } catch (Exception e) {
         safeRollback(0);
      }

      printDataContainer();
      assertNoTransactions();
   }

   public void testConflictingTxs() throws Exception {
      assertAtLeastCaches(2);
      rewireMagicKeyAwareConsistentHash();


      Object cache0Key = newKey(0, 1);
      Object cache1Key = newKey(1, 0);

      logKeysUsedInTest("testConflictingTxs", cache0Key, cache1Key);

      assertKeyOwners(cache0Key, 0, 1);
      assertKeyOwners(cache1Key, 1, 0);
      assertCacheValuesNull(cache0Key, cache1Key);

      tm(0).begin();
      cache(0).markAsWriteTransaction();
      txPut(0, cache0Key, VALUE_1, null);
      txPut(0, cache1Key, VALUE_1, null);
      tm(0).commit();


      tm(0).begin();
      cache(0).markAsWriteTransaction();
      Object value = cache(0).get(cache0Key);
      assertEquals(VALUE_1, value);
      Transaction tx0 = tm(0).suspend();

      tm(1).begin();
      cache(1).markAsWriteTransaction();
      txPut(1, cache0Key, VALUE_2, VALUE_1);
      txPut(1, cache1Key, VALUE_2, VALUE_1);
      tm(1).commit();

      tm(0).resume(tx0);
      try{
         txPut(0, cache0Key, VALUE_3, VALUE_1);
         txPut(0, cache1Key, VALUE_3, VALUE_1);
         tm(0).commit();
         assert false : "Expected to abort conflicting transaction";
      } catch (Exception e) {
         safeRollback(0);
      }

      printDataContainer();
      assertNoTransactions();
   }



   @Override
   protected void decorate(ConfigurationBuilder builder) {
      builder.clustering().hash().numOwners(1);
   }

   @Override
   protected int initialClusterSize() {
      return 2;
   }

   @Override
   protected boolean syncCommitPhase() {
      return true;
   }

   public void testWaitingForLocalCommit() throws Exception {
      if (cacheManagers.size() > 2) {
         //Note: this test is not determinist with more than 2 caches
         return;
      }
      assertAtLeastCaches(2);
      rewireMagicKeyAwareConsistentHash();

      final DelayCommit delayCommit = addDelayCommit(0, 5000);
      final ObtainTransactionEntry obtainTransactionEntry = new ObtainTransactionEntry(cache(1));

      final Object cache0Key = newKey(0, 1);
      final Object cache1Key = newKey(1, 0);

      logKeysUsedInTest("testWaitingForLocalCommit", cache0Key, cache1Key);

      assertKeyOwners(cache0Key, 0, 1);
      assertKeyOwners(cache1Key, 1, 0);
      assertCacheValuesNull(cache0Key, cache1Key);

      tm(0).begin();
      txPut(0, cache0Key, VALUE_1, null);
      txPut(0, cache1Key, VALUE_1, null);
      tm(0).commit();

      Thread otherThread = new Thread("TestWaitingForLocalCommit-Thread") {
         @Override
         public void run() {
            try {
               tm(1).begin();
               cache(1).markAsWriteTransaction();
               txPut(1, cache0Key, VALUE_2, VALUE_1);
               txPut(1, cache1Key, VALUE_2, VALUE_1);
               obtainTransactionEntry.expectedThisThread();
               delayCommit.blockTransaction(globalTransaction(1));
               tm(1).commit();
            } catch (Exception e) {
               e.printStackTrace();
            }
         }
      };
      obtainTransactionEntry.reset();
      otherThread.start();
      TransactionEntry transactionEntry = obtainTransactionEntry.getTransactionEntry();
      transactionEntry.awaitUntilCommitted(null);

      tm(0).begin();
      assertEquals(VALUE_2, cache(0).get(cache1Key));
      assertEquals(VALUE_2, cache(0).get(cache0Key));
      tm(0).commit();

      delayCommit.unblock();
      otherThread.join();

      printDataContainer();
      assertNoTransactions();
      cache(0).getAdvancedCache().removeInterceptor(DelayCommit.class);
      cache(1).getAdvancedCache().removeInterceptor(ObtainTransactionEntry.class);
   }

   public void testWaitingForLocalCommitExploreSSI() throws Exception {
      if (cacheManagers.size() > 2) {
         //Note: this test is not determinist with more than 2 caches
         return;
      }
      assertAtLeastCaches(2);
      rewireMagicKeyAwareConsistentHash();

      final DelayCommit delayCommit = addDelayCommit(0, 5000);
      final ObtainTransactionEntry obtainTransactionEntry = new ObtainTransactionEntry(cache(1));

      final Object cache0Key = newKey(0, 1);
      final Object cache1Key = newKey(1, 0);

      logKeysUsedInTest("testWaitingForLocalCommit", cache0Key, cache1Key);

      assertKeyOwners(cache0Key, 0, 1);
      assertKeyOwners(cache1Key, 1, 0);
      assertCacheValuesNull(cache0Key, cache1Key);

      tm(0).begin();
      cache(0).markAsWriteTransaction();
      txPut(0, cache0Key, VALUE_1, null);
      txPut(0, cache1Key, VALUE_1, null);
      tm(0).commit();

      Thread otherThread = new Thread("TestWaitingForLocalCommit-Thread") {
         @Override
         public void run() {
            try {
               tm(1).begin();
               cache(1).markAsWriteTransaction();
               txPut(1, cache0Key, VALUE_2, VALUE_1);
               txPut(1, cache1Key, VALUE_2, VALUE_1);
               obtainTransactionEntry.expectedThisThread();
               delayCommit.blockTransaction(globalTransaction(1));
               tm(1).commit();
            } catch (Exception e) {
               e.printStackTrace();
            }
         }
      };
      obtainTransactionEntry.reset();
      otherThread.start();
      TransactionEntry transactionEntry = obtainTransactionEntry.getTransactionEntry();
      transactionEntry.awaitUntilCommitted(null);

      tm(0).begin();
      assertEquals(VALUE_2, cache(0).get(cache1Key));
      assertEquals(VALUE_2, cache(0).get(cache0Key));

      delayCommit.unblock();
      otherThread.join();
      
      tm(0).commit();

      printDataContainer();
      assertNoTransactions();
      cache(0).getAdvancedCache().removeInterceptor(DelayCommit.class);
      cache(1).getAdvancedCache().removeInterceptor(ObtainTransactionEntry.class);
   }
   
   public void testWaitInRemoteNode() throws Exception {
      //      if (cacheManagers.size() > 2) {
      //         //Note: this test is not determinist with more than 2 caches
      //         return;
      //      }
      //      assertAtLeastCaches(2);
      //      rewireMagicKeyAwareConsistentHash();
      //
      //      final DelayCommit delayCommit = addDelayCommit(0, 5000);
      //
      //      final ObtainTransactionEntry obtainTransactionEntry = new ObtainTransactionEntry(cache(1));
      //
      //      final Object cache0Key = newKey(0, 1);
      //      final Object cache1Key = newKey(1, 0);
      //
      //      logKeysUsedInTest("testWaitInRemoteNode", cache0Key, cache1Key);
      //
      //      assertKeyOwners(cache0Key, 0, 1);
      //      assertKeyOwners(cache1Key, 1, 0);
      //      assertCacheValuesNull(cache0Key, cache1Key);
      //
      //      tm(0).begin();
      //      txPut(0, cache0Key, VALUE_1, null);
      //      txPut(0, cache1Key, VALUE_1, null);
      //      tm(0).commit();
      //
      //      Thread otherThread = new Thread("TestWaitingForLocalCommit-Thread") {
      //         @Override
      //         public void run() {
      //            try {
      //               tm(1).begin();
      //               txPut(1, cache0Key, VALUE_2, VALUE_1);
      //               txPut(1, cache1Key, VALUE_2, VALUE_1);
      //               delayCommit.blockTransaction(globalTransaction(1));
      //               obtainTransactionEntry.expectedThisThread();
      //               tm(1).commit();
      //            } catch (Exception e) {
      //               e.printStackTrace();
      //            }
      //         }
      //      };
      //      obtainTransactionEntry.reset();
      //      otherThread.start();
      //      TransactionEntry transactionEntry = obtainTransactionEntry.getTransactionEntry();
      //      transactionEntry.awaitUntilCommitted(null);
      //
      //      //tx already committed in cache(1). start a read only on cache(1) reading the local key and them the remote key.
      //      // the remote get should wait until the transaction is committed
      //      tm(1).begin();
      //      assertEquals(VALUE_2, cache(1).get(cache1Key));
      //      assertEquals(VALUE_2, cache(1).get(cache0Key));
      //      tm(1).commit();
      //
      //      delayCommit.unblock();
      //      otherThread.join();
      //
      //      printDataContainer();
      //      assertNoTransactions();
      //      cache(0).getAdvancedCache().removeInterceptor(DelayCommit.class);
      //      cache(1).getAdvancedCache().removeInterceptor(ObtainTransactionEntry.class);
   }

   @Override
   protected CacheMode cacheMode() {
      return CacheMode.DIST_SYNC;
   }
}
