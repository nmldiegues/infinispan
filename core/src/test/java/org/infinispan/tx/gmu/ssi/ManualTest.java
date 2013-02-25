package org.infinispan.tx.gmu.ssi;

import java.util.Arrays;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

import com.arjuna.ats.arjuna.common.arjPropertyManager;

@Test(groups = "functional", testName = "tx.gmu.ssi.ManualTest")
public class ManualTest extends AbstractSSITest {

   @Override
   protected void decorate(ConfigurationBuilder builder) {
      builder.clustering().hash().numOwners(1);
      builder.clustering().sync().replTimeout(1000000);
      arjPropertyManager.getCoordinatorEnvironmentBean().setDefaultTimeout(100000000);
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

   // Write contention even with transactions in the middle should abort
   public void test01() throws Exception {
      assertAtLeastCaches(3);
      rewireMagicKeyAwareConsistentHash();

      final Object x = newKey(2, Arrays.asList(0, 1));
      final Object y= newKey(2, Arrays.asList(0, 1));

      logKeysUsedInTest("test9", x, y);

      assertKeyOwners(x, 2, Arrays.asList(0, 1));
      assertKeyOwners(y, 2, Arrays.asList(0, 1));
      assertCacheValuesNull(x);
      assertCacheValuesNull(y);

      tm(2).begin();
      cache(2).markAsWriteTransaction();
      txPut(2, x, INIT, null);
      txPut(2, y, INIT, null);
      tm(2).commit();
      
      new Thread() {
         public void run() {
            try {
               tm(2).begin();
               cache(2).markAsWriteTransaction();
               cache(2).put(x,  "A");
               Thread.sleep(4000);
               tm(2).commit();
            } catch (Exception e) {
               throw new RuntimeException(e);
            }
         }
      }.start();
      
      tm(2).begin();
      cache(2).markAsWriteTransaction();
      cache(2).put(y,  "B");
      tm(2).commit();   // This commit must take place after the VR of 'A' in 'x'
      
      tm(2).begin();
      cache(2).markAsWriteTransaction();
      System.out.println("C read previous version for x: " + cache(2).put(x, "C"));
      tm(2).commit();   // This commit must take place after 'A's commit
   }
   
   // Read-only reads from time-warp committed
//   public void test7() throws Exception {
//      assertAtLeastCaches(3);
//      rewireMagicKeyAwareConsistentHash();
//
//      final Object x = newKey(2, Arrays.asList(0, 1)); // C = 2
//      final Object y = newKey(0, Arrays.asList(1, 2)); // A = 0
//      final Object w = newKey(2, Arrays.asList(0, 1)); // C = 2
//      final Object z = newKey(0, Arrays.asList(1, 2)); // A = 0
//      System.err.println(x + " " + y + " " + w + " " + z);
//
//      logKeysUsedInTest("test7", x, y, w, z);
//
//      assertKeyOwners(x, 2, Arrays.asList(0, 1));
//      assertKeyOwners(y, 0, Arrays.asList(1, 2));
//      assertKeyOwners(w, 2, Arrays.asList(0, 1));
//      assertKeyOwners(z, 0, Arrays.asList(1, 2));
//      assertCacheValuesNull(x, y, w, z);
//
//      // [B, C, A]
//      
//      tm(0).begin();
//      cache(0).markAsWriteTransaction();
//      txPut(0, x, INIT, null); // VR: 15@C; new body: [2, 16, 16] version 16
//      txPut(0, y, INIT, null); // VR: 15@A; new body: [2, 16, 16] version 16
//      txPut(0, w, INIT, null); // VR: 15@C; new body: [2, 16, 16] version 16
//      txPut(0, z, INIT, null); // VR: 15@A; new body: [2, 16, 16] version 16
//      tm(0).commit(); // out flag: false 2PC commit time: [2, 16, 16] computed deps: [2, 16, 16]
//      
//      tm(1).begin();
//      cache(1).markAsWriteTransaction();
//      assert INIT.equals(cache(1).get(w)); // VR: 16@C
//      Transaction B = tm(1).suspend();
//      
//      tm(2).begin();
//      cache(2).markAsWriteTransaction();
//      cache(2).put(w, "C"); // VR: 16@C; new body: [2, 17, 16] version 17
//      tm(2).commit(); // prepare [2, 16, 16]; commit alone [2, 17, 16]
//      
//      tm(0).begin();
//      // this should force the RO to get a snapshot on y's node (A/0, but not on C/2)
//      assert INIT.equals(cache(0).get(z)); // VR: 16@A
//      Transaction RO = tm(0).suspend();
//      
//      tm(1).resume(B);
//      cache(1).put(x, "B"); // VR: 17@C; new body: [2, 18, 17] version 17
//      cache(1).put(y, "B"); // VR: 16@A; new body: [2, 18, 17] version 17
//      tm(1).commit(); // prepare [2, 16, 16]; commit [2, 18, 17], computed deps [2, 17, 17]
//      
//      tm(0).resume(RO);
//      assert "B".equals(cache(0).get(y)); // VR: 18@A
//      tm(0).commit();
      
//   }
   

}
