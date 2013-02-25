package org.infinispan.tx.gmu.ssi;

import static junit.framework.Assert.assertEquals;

import javax.transaction.Transaction;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

import com.arjuna.ats.arjuna.common.arjPropertyManager;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
@Test(groups = "functional", testName = "tx.gmu.ssi.GMUEquivalentTest")
public class GMUEquivalentTest extends AbstractGMUTest {

//   public void testGetSnapshot() throws Exception {
//      assertAtLeastCaches(2);
//      rewireMagicKeyAwareConsistentHash();
//
//      Object cache0Key0 = newKey(0, 1);
//      Object cache1Key1 = newKey(1, 0);
//      Object cache1Key2 = newKey(1, 0);
//
//      logKeysUsedInTest("testGetSnapshot", cache0Key0, cache1Key1, cache1Key2);
//
//      assertKeyOwners(cache0Key0, 0, 1);
//      assertKeyOwners(cache1Key1, 1, 0);
//      assertKeyOwners(cache1Key2, 1, 0);
//      assertCacheValuesNull(cache0Key0, cache1Key1, cache1Key2);
//
//      tm(0).begin();
//      cache(0).markAsWriteTransaction();
//      txPut(0, cache0Key0, VALUE_1, null);
//      txPut(0, cache1Key1, VALUE_1, null);
//      txPut(0, cache1Key2, VALUE_1, null);
//      tm(0).commit();
//
//      assertCachesValue(0, cache0Key0, VALUE_1);
//      assertCachesValue(0, cache1Key1, VALUE_1);
//      assertCachesValue(0, cache1Key2, VALUE_1);
//
//      tm(0).begin();
//      assert VALUE_1.equals(cache(0).get(cache0Key0));
//      Transaction tx0 = tm(0).suspend();
//
//      tm(1).begin();
//      cache(1).markAsWriteTransaction();
//      cache(1).put(cache0Key0, VALUE_2);
//      cache(1).put(cache1Key1, VALUE_2);
//      cache(1).put(cache1Key2, VALUE_2);
//      tm(1).commit();
//
//      assertCachesValue(1, cache0Key0, VALUE_2);
//      assertCachesValue(1, cache1Key1, VALUE_2);
//      assertCachesValue(1, cache1Key2, VALUE_2);
//
//      tm(0).resume(tx0);
//      assert VALUE_1.equals(cache(0).get(cache0Key0));
//      assert VALUE_1.equals(cache(0).get(cache1Key1));
//      assert VALUE_1.equals(cache(0).get(cache1Key2));
//      tm(0).commit();
//
//      assertNoTransactions();
//      printDataContainer();
//   }
//
//   public void testPrematureAbort() throws Exception {
//      assertAtLeastCaches(2);
//      rewireMagicKeyAwareConsistentHash();
//
//      Object cache0Key = newKey(0, 1);
//      Object cache1Key = newKey(1, 0);
//
//      logKeysUsedInTest("testPrematureAbort", cache0Key, cache1Key);
//
//      assertKeyOwners(cache0Key, 0, 1);
//      assertKeyOwners(cache1Key, 1, 0);
//      assertCacheValuesNull(cache0Key, cache1Key);
//
//      tm(0).begin();
//      cache(0).markAsWriteTransaction();
//      txPut(0, cache0Key, VALUE_1, null);
//      txPut(0, cache1Key, VALUE_1, null);
//      tm(0).commit();
//
//
//      tm(0).begin();
//      cache(0).markAsWriteTransaction();
//      Object value = cache(0).get(cache0Key);
//      assertEquals(VALUE_1, value);
//      Transaction tx0 = tm(0).suspend();
//
//      tm(1).begin();
//      cache(1).markAsWriteTransaction();
//      txPut(1, cache0Key, VALUE_2, VALUE_1);
//      txPut(1, cache1Key, VALUE_2, VALUE_1);
//      tm(1).commit();
//
//      tm(0).resume(tx0);
//      try{
//         txPut(0, cache0Key, VALUE_3, VALUE_1);
//         cache(0).get(cache1Key);
//         tm(0).commit();
//         assert false : "Expected to abort conflicting transaction";
//      } catch (Exception e) {
//         safeRollback(0);
//      }
//
//      printDataContainer();
//      assertNoTransactions();
//   }
//
//   public void testConflictingTxs() throws Exception {
//      assertAtLeastCaches(2);
//      rewireMagicKeyAwareConsistentHash();
//
//
//      Object cache0Key = newKey(0, 1);
//      Object cache1Key = newKey(1, 0);
//
//      logKeysUsedInTest("testConflictingTxs", cache0Key, cache1Key);
//
//      assertKeyOwners(cache0Key, 0, 1);
//      assertKeyOwners(cache1Key, 1, 0);
//      assertCacheValuesNull(cache0Key, cache1Key);
//
//      tm(0).begin();
//      cache(0).markAsWriteTransaction();
//      txPut(0, cache0Key, VALUE_1, null);
//      txPut(0, cache1Key, VALUE_1, null);
//      tm(0).commit();
//
//
//      tm(0).begin();
//      cache(0).markAsWriteTransaction();
//      Object value = cache(0).get(cache0Key);
//      assertEquals(VALUE_1, value);
//      Transaction tx0 = tm(0).suspend();
//
//      tm(1).begin();
//      cache(1).markAsWriteTransaction();
//      txPut(1, cache0Key, VALUE_2, VALUE_1);
//      txPut(1, cache1Key, VALUE_2, VALUE_1);
//      tm(1).commit();
//
//      tm(0).resume(tx0);
//      try{
//         txPut(0, cache0Key, VALUE_3, VALUE_1);
//         txPut(0, cache1Key, VALUE_3, VALUE_1);
//         tm(0).commit();
//         assert false : "Expected to abort conflicting transaction";
//      } catch (Exception e) {
//         safeRollback(0);
//      }
//
//      printDataContainer();
//      assertNoTransactions();
//   }



   @Override
   protected void decorate(ConfigurationBuilder builder) {
      builder.clustering().hash().numOwners(1);
      arjPropertyManager.getCoordinatorEnvironmentBean().setDefaultTimeout(100000000);
   }

   @Override
   protected int initialClusterSize() {
      return 2;
   }

   @Override
   protected boolean syncCommitPhase() {
      return true;
   }

   public void testWaitingForLocalCommitExploreSSI() throws Exception {
      if (cacheManagers.size() > 2) {
         //Note: this test is not determinist with more than 2 caches
         return;
      }
      assertAtLeastCaches(2);
      rewireMagicKeyAwareConsistentHash();

      final Object cache0Key = newKey(0, 1);
      final Object cache1Key = newKey(1, 0);
      final Object cache2Key = newKey(1, 0);

      logKeysUsedInTest("testWaitingForLocalCommit", cache0Key, cache1Key, cache2Key);

      assertKeyOwners(cache0Key, 0, 1);
      assertKeyOwners(cache1Key, 1, 0);
      assertKeyOwners(cache2Key, 1, 0);
      assertCacheValuesNull(cache0Key, cache1Key, cache2Key);

      tm(0).begin();
      cache(0).markAsWriteTransaction();
      txPut(0, cache0Key, VALUE_1, null);
      txPut(0, cache1Key, VALUE_1, null);
      tm(0).commit();

      tm(0).begin();
      cache(0).markAsWriteTransaction();
      assertEquals(VALUE_1, cache(0).get(cache0Key));
      assertEquals(VALUE_1, cache(0).get(cache1Key));
      txPut(0, cache2Key, VALUE_1, null);
      Transaction tx0 = tm(0).suspend();

      tm(1).begin();
      cache(1).markAsWriteTransaction();
      txPut(1, cache0Key, VALUE_2, VALUE_1);
      txPut(1, cache1Key, VALUE_2, VALUE_1);
      tm(1).commit();

      tm(0).resume(tx0);
      tm(0).commit();

      printDataContainer();
      assertNoTransactions();
   }

//   public void testCycleDetectionRWs() throws Exception {
//      if (cacheManagers.size() > 2) {
//         //Note: this test is not determinist with more than 2 caches
//         return;
//      }
//      assertAtLeastCaches(2);
//      rewireMagicKeyAwareConsistentHash();
//
//      final Object cache0Key = newKey(0, 1);
//      final Object cache1Key = newKey(1, 0);
//      final Object cache2Key = newKey(1, 0);
//
//      logKeysUsedInTest("testWaitingForLocalCommit", cache0Key, cache1Key);
//
//      assertKeyOwners(cache0Key, 0, 1);
//      assertKeyOwners(cache1Key, 1, 0);
//      assertCacheValuesNull(cache0Key, cache1Key);
//
//      tm(0).begin();
//      cache(0).markAsWriteTransaction();
//      txPut(0, cache0Key, VALUE_1, null);
//      txPut(0, cache1Key, VALUE_1, null);
//      tm(0).commit();
//
//      tm(0).begin();
//      cache(0).markAsWriteTransaction();
//      assertEquals(VALUE_1, cache(0).get(cache0Key));
//      assertEquals(VALUE_1, cache(0).get(cache1Key));
//      txPut(0, cache2Key, VALUE_2, null);
//      Transaction tx0 = tm(0).suspend();
//
//      tm(1).begin();
//      cache(1).markAsWriteTransaction();
//      txPut(1, cache0Key, VALUE_2, VALUE_1);
//      txPut(1, cache1Key, VALUE_2, VALUE_1);
//      tm(1).commit();
//
//      tm(0).resume(tx0);
//      tm(0).commit();
//
//      printDataContainer();
//      assertNoTransactions();
//   }
//   
//   public void testComputeDependencies() throws Exception {
//      if (cacheManagers.size() > 2) {
//         return;
//      }
//      assertAtLeastCaches(2);
//      rewireMagicKeyAwareConsistentHash();
//
//      final Object x = newKey(0, 1); // @ R0
//      final Object y = newKey(1, 0); // @ R1
//
//      logKeysUsedInTest("testComputeDependencies", x, y);
//
//      assertKeyOwners(x, 0, 1);
//      assertKeyOwners(y, 1, 0);
//      assertCacheValuesNull(x, y);
//
//      tm(0).begin();
//      cache(0).markAsWriteTransaction();
//      txPut(0, x, VALUE_1, null); // VR: 0 @ R0
//      txPut(0, y, VALUE_1, null); // VR: 0 @ R1
//      System.err.println("==> Committing on 0 INIT");
//      tm(0).commit(); // prepare R0: 0; R1: 0      |      R0: 1; R1: 1  
//
//      tm(0).begin();
//      cache(0).markAsWriteTransaction();
//      cache(0).put(y, "A"); // VR: 1 @ R1
//      Transaction A = tm(0).suspend();
//
//      tm(1).begin();
//      cache(1).markAsWriteTransaction();
//      assertEquals(VALUE_1, cache(1).get(y)); // VR: 2 @ R1, missed concurrent write
//      cache(1).put(x, "B"); // VR: 1 @ R0
//      Transaction B = tm(1).suspend();
//
//      tm(0).resume(A);
//      System.err.println("==> Committing on 0");
//      tm(0).commit(); // prepare R0: 1; R1: 1      |      R0: 1; R1: 2
//      
//      tm(1).resume(B);
//      System.err.println("==> Committing on 1");
//      tm(1).commit(); // prepare R0: 1; R1: 1      |      R0: 2; R1: 2
//
//      printDataContainer();
//      assertNoTransactions();
//   }
   
   @Override
   protected CacheMode cacheMode() {
      return CacheMode.DIST_SYNC;
   }
}
