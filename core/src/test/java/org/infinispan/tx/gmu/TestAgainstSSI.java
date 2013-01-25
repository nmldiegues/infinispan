package org.infinispan.tx.gmu;

import static junit.framework.Assert.assertEquals;

import javax.transaction.Transaction;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
@Test(groups = "functional", testName = "tx.gmu.TestAgainstSSI")
public class TestAgainstSSI extends ConsistencyTest {

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

      logKeysUsedInTest("testWaitingForLocalCommit", cache0Key, cache1Key);

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
      assertEquals(VALUE_1, cache(0).get(cache0Key));
      assertEquals(VALUE_1, cache(0).get(cache1Key));
      txPut(0, cache2Key, VALUE_2, null);
      Transaction tx0 = tm(0).suspend();
      
      tm(1).begin();
      cache(1).markAsWriteTransaction();
      txPut(1, cache0Key, VALUE_2, VALUE_1);
      txPut(1, cache1Key, VALUE_2, VALUE_1);
      tm(1).commit();

      tm(0).resume(tx0);
      try{
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
   protected CacheMode cacheMode() {
      return CacheMode.DIST_SYNC;
   }
}
