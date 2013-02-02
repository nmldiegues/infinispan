package org.infinispan.tx.gmu.ssi;

import java.util.Arrays;

import javax.transaction.Transaction;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "tx.gmu.ssi.TWMTest")
public class TWMTest extends AbstractSSITest {

   @Override
   protected void decorate(ConfigurationBuilder builder) {
      builder.clustering().hash().numOwners(1);
      builder.clustering().sync().replTimeout(1000000);
   }

   @Override
   protected int initialClusterSize() {
      return 3;
   }

   @Override
   protected boolean syncCommitPhase() {
      return true;
   }

   @Override
   protected CacheMode cacheMode() {
      return CacheMode.DIST_SYNC;
   }

   private static final String INIT = "init";

   // Basic dangerous structure across 2 nodes
   public void test1() throws Exception {
      assertAtLeastCaches(2);
      rewireMagicKeyAwareConsistentHash();

      final Object keyX = newKey(1, 0);
      final Object keyY = newKey(0, 1);
      final Object keyZ = newKey(1, 0);
      final Object keyO = newKey(1, 0);

      logKeysUsedInTest("test1", keyX, keyY, keyZ);

      assertKeyOwners(keyX, 1, 0);
      assertKeyOwners(keyY, 0, 1);
      assertKeyOwners(keyZ, 1, 0);
      assertKeyOwners(keyO, 1, 0);
      assertCacheValuesNull(keyX, keyY, keyZ, keyO);

      tm(0).begin();
      cache(0).markAsWriteTransaction();
      txPut(0, keyX, INIT, null);
      txPut(0, keyY, INIT, null);
      txPut(0, keyZ, INIT, null);
      txPut(0, keyO, INIT, null);
      tm(0).commit();

      tm(0).begin();
      System.err.println("Started A on 0");
      cache(0).markAsWriteTransaction();
      txPut(0, keyX, "A", INIT);
      txPut(0, keyZ, "A", INIT);
      Transaction txA = tm(0).suspend();

      tm(1).begin();
      System.err.println("Started B on 1");
      cache(1).markAsWriteTransaction();
      // this is used to force B to get a snapshot on 0 before A commits
      // it is hosted in 1 because 1 is where x is hosted (to where A wrote)
      assert INIT.equals(cache(1).get(keyO));
      Transaction txB = tm(1).suspend();

      tm(0).resume(txA);
      System.err.println("Committed A on 0");
      tm(0).commit();

      tm(0).begin();
      cache(0).markAsWriteTransaction();
      assert INIT.equals(cache(0).get(keyO));
      Transaction txC = tm(0).suspend();

      tm(1).resume(txB);
      String tmp = (String) cache(1).get(keyX);
      System.err.println("B read X: " + tmp);
      assert INIT.equals(tmp);
      txPut(1, keyY, "B", INIT);
      tm(1).commit();

      tm(0).resume(txC);
      assert "A".equals(cache(0).get(keyZ));
      assert INIT.equals(cache(0).get(keyY));
      try {
         tm(0).commit();
         assert false : "Expected to abort conflicting transaction";
      } catch (Exception e) {}

      printDataContainer();
      assertNoTransactions();
   }

}
