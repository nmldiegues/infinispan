package org.infinispan.tx;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.dataplacement.DataPlacementManager;
import org.infinispan.dataplacement.hm.HashMapObjectLookupFactory;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.reconfigurableprotocol.manager.ReconfigurableReplicationManager;
import org.infinispan.reconfigurableprotocol.protocol.TotalOrderCommitProtocol;
import org.infinispan.reconfigurableprotocol.protocol.TwoPhaseCommitProtocol;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

import static org.infinispan.test.TestingUtil.extractComponent;
import static org.infinispan.test.TestingUtil.sleepThread;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
@Test(groups = "functional", testName = "tx.SelfTunningTest")
public class SelfTunningTest extends MultipleCacheManagersTest {

   private static final String KEY_1 = "KEY_1";
   private static final String KEY_2 = "KEY_2";
   private static final String KEY_3 = "KEY_3";
   private static final String VALUE = "VALUE";
   private final ConfigurationBuilder builder;

   public SelfTunningTest() {
      builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      cleanup = CleanupPhase.AFTER_METHOD;
   }

   public void testReplicationDegree() throws Exception {
      populate();
      DistributionManager cache0DM = extractComponent(cache(0), DistributionManager.class);
      DistributionManager cache1DM = extractComponent(cache(1), DistributionManager.class);
      DataPlacementManager dataPlacementManager = extractComponent(cache(0), DataPlacementManager.class);

      assertReplicationDegree(cache0DM, 1);
      assertReplicationDegree(cache1DM, 1);

      triggerTunningReplicationDegree(dataPlacementManager, 2);

      assertReplicationDegree(cache0DM, 2);
      assertReplicationDegree(cache1DM, 2);

      addClusterEnabledCacheManager(builder);
      waitForClusterToForm();

      assertReplicationDegree(cache0DM, 2);
      assertReplicationDegree(cache1DM, 2);
      assertReplicationDegree(extractComponent(cache(2), DistributionManager.class), 2);
      assertKeysValue();
   }

   public void testSwitch() throws Exception {
      populate();
      ReconfigurableReplicationManager manager0 = extractComponent(cache(0), ReconfigurableReplicationManager.class);
      ReconfigurableReplicationManager manager1 = extractComponent(cache(1), ReconfigurableReplicationManager.class);

      assertProtocol(manager0, TwoPhaseCommitProtocol.UID);
      assertEpoch(manager0, 0);
      assertProtocol(manager1, TwoPhaseCommitProtocol.UID);
      assertEpoch(manager1, 0);

      triggerTunningProtocol(manager0, TotalOrderCommitProtocol.UID);

      assertProtocol(manager0, TotalOrderCommitProtocol.UID);
      assertEpoch(manager0, 1);
      assertProtocol(manager1, TotalOrderCommitProtocol.UID);
      assertEpoch(manager1, 1);

      addClusterEnabledCacheManager(builder);
      waitForClusterToForm();

      ReconfigurableReplicationManager manager2 = extractComponent(cache(2), ReconfigurableReplicationManager.class);

      assertProtocol(manager0, TotalOrderCommitProtocol.UID);
      assertEpoch(manager0, 1);
      assertProtocol(manager1, TotalOrderCommitProtocol.UID);
      assertEpoch(manager1, 1);
      assertProtocol(manager2, TotalOrderCommitProtocol.UID);
      assertEpoch(manager2, 1);
      assertKeysValue();
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      builder.clustering().hash().numOwners(1);
      builder.dataPlacement().enabled(true)
            .objectLookupFactory(new HashMapObjectLookupFactory())
            .coolDownTime(1000);
      builder.clustering().stateTransfer().fetchInMemoryState(true);
      createCluster(builder, 2);
      waitForClusterToForm();
   }

   private void assertReplicationDegree(final DistributionManager distributionManager, final int expectedReplicationDegree) {
      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return distributionManager.getReplicationDegree() == expectedReplicationDegree;
         }
      });
   }

   private void assertProtocol(final ReconfigurableReplicationManager manager, final String protocolId) {
      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return manager.getCurrentProtocolId().equals(protocolId);
         }
      });
   }

   private void assertEpoch(final ReconfigurableReplicationManager manager, final long epoch) {
      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return epoch == manager.getCurrentEpoch();
         }
      });
   }

   private void triggerTunningReplicationDegree(DataPlacementManager dataPlacementManager, int replicationDegree) throws Exception {
      sleepThread(DataPlacementManager.INITIAL_COOL_DOWN_TIME);
      dataPlacementManager.setReplicationDegree(replicationDegree);
   }

   private void triggerTunningProtocol(ReconfigurableReplicationManager manager, String protocolId) throws Exception {
      sleepThread(manager.getSwitchCoolDownTime() * 1000);
      manager.switchTo(protocolId, false, false);
   }

   private void populate() {
      cache(0).put(KEY_1, VALUE);
      cache(0).put(KEY_2, VALUE);
      cache(0).put(KEY_3, VALUE);
   }

   private void assertKeysValue() {
      for (final Cache cache : caches()) {
         eventually(new Condition() {
            @Override
            public boolean isSatisfied() throws Exception {
               return VALUE.equals(cache.get(KEY_1));
            }
         });
         eventually(new Condition() {
            @Override
            public boolean isSatisfied() throws Exception {
               return VALUE.equals(cache.get(KEY_2));
            }
         });
         eventually(new Condition() {
            @Override
            public boolean isSatisfied() throws Exception {
               return VALUE.equals(cache.get(KEY_3));
            }
         });
      }
   }
}
