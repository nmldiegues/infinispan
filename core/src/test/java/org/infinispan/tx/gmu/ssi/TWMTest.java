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
      final Object keyP = newKey(0, 1);

      logKeysUsedInTest("test1", keyX, keyY, keyZ);

      assertKeyOwners(keyX, 1, 0);
      assertKeyOwners(keyY, 0, 1);
      assertKeyOwners(keyZ, 1, 0);
      assertKeyOwners(keyO, 1, 0);
      assertKeyOwners(keyP, 0, 1);
      assertCacheValuesNull(keyX, keyY, keyZ, keyO, keyP);

      tm(0).begin();
      cache(0).markAsWriteTransaction();
      txPut(0, keyX, INIT, null);
      txPut(0, keyY, INIT, null);
      txPut(0, keyZ, INIT, null);
      txPut(0, keyO, INIT, null);
      txPut(0, keyP, INIT, null);
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
      System.err.println("Started C on 0");
      cache(0).markAsWriteTransaction();
      // similar to above, but we need P to be hosted by 0 because that's where
      // we need C to miss a write
      assert INIT.equals(cache(0).get(keyP));
      Transaction txC = tm(0).suspend();

      tm(1).resume(txB);
      String tmp = (String) cache(1).get(keyX);
      System.err.println("B read X: " + tmp);
      assert INIT.equals(tmp);
      txPut(1, keyY, "B", INIT);
      System.err.println("Committed B on 1");
      tm(1).commit();

      tm(0).resume(txC);
      assert "A".equals(cache(0).get(keyZ));
      assert INIT.equals(cache(0).get(keyY));
      try {
         System.err.println("Committed C on 0");
         tm(0).commit();
         assert false : "Expected to abort conflicting transaction";
      } catch (Exception e) {}

      printDataContainer();
      assertNoTransactions();
   }

   // Try to make snapshot validation fail due to concurrent write being time-warp committed
   public void test2() throws Exception {
      assertAtLeastCaches(3);
      rewireMagicKeyAwareConsistentHash();

      final Object x = newKey(2, Arrays.asList(0, 1));
      final Object y = newKey(0, Arrays.asList(1, 2));
      final Object w = newKey(2, Arrays.asList(0, 1));
      final Object z = newKey(0, Arrays.asList(1, 2));

      logKeysUsedInTest("test2", x, y, w, z);

      assertKeyOwners(x, 2, Arrays.asList(0, 1));
      assertKeyOwners(y, 0, Arrays.asList(1, 2));
      assertKeyOwners(w, 2, Arrays.asList(0, 1));
      assertKeyOwners(z, 0, Arrays.asList(1, 2));
      assertCacheValuesNull(x, y, w, z);

      tm(0).begin();
      cache(0).markAsWriteTransaction();
      txPut(0, x, INIT, null);
      txPut(0, y, INIT, null);
      txPut(0, w, INIT, null);
      txPut(0, z, INIT, null);
      tm(0).commit();
      
      tm(0).begin();
      cache(0).markAsWriteTransaction();
      cache(0).put(x, "A");
      Transaction A = tm(0).suspend();
      
      tm(1).begin();
      cache(1).markAsWriteTransaction();
      assert INIT.equals(cache(1).get(x));
      cache(1).put(y, "B");
      cache(1).put(w, "B");
      Transaction B = tm(1).suspend();
      
      tm(2).begin();
      cache(2).markAsWriteTransaction();
      cache(2).put(y, "D");
      Transaction D = tm(2).suspend();
      
      tm(0).resume(A);
      tm(0).commit();
      
      tm(2).resume(D);
      tm(2).commit();
      
      tm(2).begin();
      assert "D".equals(cache(2).get(y));
      cache(2).put(z, "C");
      Transaction C = tm(2).suspend();
      
      tm(1).resume(B);
      try {
         tm(1).commit();
         assert false : "Expected to abort";
      } catch (Exception e) {}
      
      // Commented the test because it cannot happen with read-before-write in GMU
      
//      tm(1).begin();
//      assert "B".equals(cache(1).get(w));
//      assert INIT.equals(cache(1).get(z));
//      tm(1).commit();
      
//      tm(2).resume(C);
//      try {
//         tm(2).commit();
//         assert false : "Expected to abort conflicting transaction";
//      } catch (Exception e) {}
   }
}
