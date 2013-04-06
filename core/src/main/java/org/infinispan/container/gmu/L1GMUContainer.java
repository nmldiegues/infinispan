package org.infinispan.container.gmu;

import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.gmu.InternalGMUCacheEntry;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.VersionGenerator;
import org.infinispan.container.versioning.gmu.GMUVersion;
import org.infinispan.container.versioning.gmu.GMUVersionGenerator;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.concurrent.IsolationLevel;
import org.rhq.helpers.pluginAnnotations.agent.DataType;
import org.rhq.helpers.pluginAnnotations.agent.DisplayType;
import org.rhq.helpers.pluginAnnotations.agent.MeasurementType;
import org.rhq.helpers.pluginAnnotations.agent.Metric;
import org.rhq.helpers.pluginAnnotations.agent.Operation;
import org.rhq.helpers.pluginAnnotations.agent.Parameter;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.Collection;
import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static org.infinispan.container.gmu.GMUEntryFactoryImpl.wrap;
import static org.infinispan.transaction.gmu.GMUHelper.toGMUVersion;
import static org.infinispan.transaction.gmu.GMUHelper.toGMUVersionGenerator;

/**
 * // TODO: Document this
 *
 * @author Hugo Pimentel
 * @author Pedro Ruivo
 * @since 5.2
 */
public class L1GMUContainer {

   private final ConcurrentHashMap<Object, L1VersionChain> l1Container;
   private final EnumMap<Stat, AtomicLong> stats;
   private Configuration configuration;
   private DistributionManager distributionManager;
   private GMUVersionGenerator gmuVersionGenerator;
   private volatile boolean statisticsEnabled;
   private boolean enabled;

   public L1GMUContainer() {
      this.l1Container = new ConcurrentHashMap<Object, L1VersionChain>();
      this.stats = new EnumMap<Stat, AtomicLong>(Stat.class);
   }

   @Inject
   public void injectConfiguration(Configuration configuration, DistributionManager distributionManager,
                                   VersionGenerator versionGenerator) {
      this.enabled = configuration.locking().isolationLevel() == IsolationLevel.SERIALIZABLE &&
            configuration.clustering().l1().enabled();
      if (!enabled) {
         return;
      }
      this.configuration = configuration;
      this.distributionManager = distributionManager;
      this.gmuVersionGenerator = toGMUVersionGenerator(versionGenerator);
   }

   @Start
   public void setStatisticEnabled() {
      if (!enabled) {
         return;
      }
      this.statisticsEnabled = configuration.jmxStatistics().enabled();
   }

   public final InternalGMUCacheEntry getValidVersion(Object key, EntryVersion txVersion, Collection<Address> readFrom) {
      if (!enabled) {
         return null;
      }
      L1VersionChain versionChain = l1Container.get(key);
      if (versionChain != null) {
         VersionEntry<L1Entry> versionEntry = findMaxVersion(versionChain, txVersion, readFrom);

         if (versionEntry.isFound()) {
            L1Entry l1Entry = versionEntry.getEntry();
            Address owner = distributionManager.getPrimaryLocation(key);

            if (isValid(l1Entry, txVersion, owner)) {
               updateStats(Stat.CACHE_HIT);
               EntryVersion updatedTxVersion = gmuVersionGenerator.mergeAndMax(txVersion, l1Entry.getCreationVersion());
               return wrap(key, l1Entry.getValue(), versionEntry.isMostRecent(), false, updatedTxVersion, null, null);
            } else {
               updateStats(Stat.CACHE_MISS_OLD_VERSION);
            }
         } else {
            updateStats(Stat.CACHE_MISS_NO_VERSION);
         }
      } else {
         updateStats(Stat.CACHE_MISS_NO_KEY);
      }
      return null;
   }

   public final void insertOrUpdate(Object key, InternalGMUCacheEntry value) {
      if (!enabled) {
         return;
      }
      L1VersionChain versionChain = l1Container.get(key);
      if (versionChain == null) {
         versionChain = new L1VersionChain();
         L1VersionChain old = l1Container.putIfAbsent(key, versionChain);
         versionChain = old == null ? versionChain : old;
      }

      versionChain.add(new L1Entry(value), false, null);
   }

   public final void handleNewEntries() {
      //TODO
   }

   public final boolean contains(Object key) {
      return l1Container.containsKey(key);
   }

   public final void clear() {
      l1Container.clear();
   }

   public final String chainToString() {
      StringBuilder stringBuilder = new StringBuilder();
      for (Map.Entry<Object, L1VersionChain> entry : l1Container.entrySet()) {
         stringBuilder.append(entry.getKey())
               .append("=>");
         entry.getValue().chainToString(stringBuilder);
         stringBuilder.append("\n");
      }
      return stringBuilder.toString();
   }

   public final VersionChain<?> getVersionChain(Object key) {
      return l1Container.get(key);
   }

   @ManagedOperation(description = "Resets statistics gathered by this component")
   @Operation(displayName = "Reset Statistics")
   public void resetStatistics() {
      for (AtomicLong atomicLong : stats.values()) {
         atomicLong.set(0);
      }
   }

   @ManagedAttribute(description = "Statistic enabled")
   @Metric(displayName = "Statistics enabled", dataType = DataType.TRAIT)
   public boolean isStatisticsEnabled() {
      return this.statisticsEnabled;
   }

   @ManagedOperation(description = "Enable/disable statistics")
   @Operation(displayName = "Enable/disable statistics")
   public void setStatisticsEnabled(@Parameter(name = "enabled", description = "Whether statistics should be enabled or disabled (true/false)") boolean enabled) {
      this.statisticsEnabled = enabled;
   }

   @ManagedAttribute(description = "Number of cache misses. Reason: only old versions")
   @Metric(displayName = "cacheMissOldVersion", measurementType = MeasurementType.TRENDSUP, displayType = DisplayType.SUMMARY)
   public long getCacheMissOldVersionsOnly() {
      return stats.get(Stat.CACHE_MISS_OLD_VERSION).get();
   }

   @ManagedAttribute(description = "Number of cache misses. Reason: only new versions")
   @Metric(displayName = "cacheMissNoVersionThatFits", measurementType = MeasurementType.TRENDSUP, displayType = DisplayType.SUMMARY)
   public long getCacheMissNoVersionThatFits() {
      return stats.get(Stat.CACHE_MISS_NO_VERSION).get();
   }

   @ManagedAttribute(description = "Number of cache misses. Reason: key not found")
   @Metric(displayName = "cacheMissKeyNotFound", measurementType = MeasurementType.TRENDSUP, displayType = DisplayType.SUMMARY)
   public long getCacheMissKeyNotFound() {
      return stats.get(Stat.CACHE_MISS_NO_KEY).get();
   }

   @ManagedAttribute(description = "Number of Caches hits")
   @Metric(displayName = "cacheHits", description = "Number of caches hit")
   public long getCacheHits() {
      return stats.get(Stat.CACHE_HIT).get();
   }

   public final void gc(GMUVersion version) {
      for (Map.Entry<Object, L1VersionChain> entry : l1Container.entrySet()) {
         entry.getValue().gc(version);
      }
   }

   private boolean isValid(L1Entry entry, EntryVersion txVersion, Address owner) {
      long entryVersionValue = toGMUVersion(entry.getReadVersion()).getVersionValue(owner);
      long txEntryVersionValue = toGMUVersion(txVersion).getVersionValue(owner);

      return entryVersionValue == GMUVersion.NON_EXISTING || txEntryVersionValue == GMUVersion.NON_EXISTING ||
            entryVersionValue >= txEntryVersionValue;
   }

   private VersionEntry<L1Entry> findMaxVersion(L1VersionChain versionChain, EntryVersion txVersion, Collection<Address> readFrom) {
      EntryVersion maxVersion = gmuVersionGenerator.calculateMaxVersionToRead(txVersion, readFrom);
      return versionChain.get(maxVersion);
   }

   private void updateStats(Stat stat) {
      if (statisticsEnabled) {
         stats.get(stat).incrementAndGet();
      }
   }

   private static enum Stat {
      CACHE_HIT,
      CACHE_MISS_OLD_VERSION,
      CACHE_MISS_NO_VERSION,
      CACHE_MISS_NO_KEY
   }

   private static class L1VersionBody extends VersionBody<L1Entry> {

      private L1VersionBody(L1Entry value) {
         super(value);
      }

      @Override
      public EntryVersion getVersion() {
         return getValue().getCreationVersion();
      }

      @Override
      public boolean isOlder(VersionBody<L1Entry> otherBody) {
         L1Entry thisEntry = getValue();
         L1Entry otherEntry = otherBody.getValue();
         if (thisEntry.getValue() == null) {
            return true;
         } else if (otherEntry.getValue() == null) {
            return false;
         }
         return isOlder(thisEntry.getValue().getVersion(), otherEntry.getValue().getVersion());
      }

      @Override
      public boolean isOlderOrEquals(EntryVersion entryVersion) {
         return isOlderOrEquals(getValue().getCreationVersion(), entryVersion);
      }

      @Override
      public boolean isEqual(VersionBody<L1Entry> otherBody) {
         L1Entry thisEntry = getValue();
         L1Entry otherEntry = otherBody.getValue();
         if (thisEntry.getValue() == null) {
            return otherEntry.getValue() == null;
         } else if (otherEntry.getValue() == null) {
            return false;
         }
         return isEqual(thisEntry.getValue().getVersion(), otherEntry.getValue().getVersion());
      }

      @Override
      public boolean isRemove() {
         return false;
      }

      @Override
      public void reincarnate(VersionBody<L1Entry> other) {
         getValue().setReadVersion(other.getValue().getReadVersion());
      }

      @Override
      public VersionBody<L1Entry> gc(EntryVersion minVersion) {
         if (minVersion == null || isOlderOrEquals(getValue().getCreationVersion(), minVersion)) {
            VersionBody<L1Entry> previous = getPrevious();
            setPrevious(null);
            return previous;
         } else {
            return getPrevious();
         }
      }

      @Override
      protected boolean isExpired(long now) {
         return false;
      }
      
      @Override
      protected boolean hasOutgoingEdge() {
         return false;
      }
   }

   private class L1VersionChain extends VersionChain<L1Entry> {

      @Override
      protected VersionBody<L1Entry> newValue(L1Entry value, boolean outFlag, long[] creatorVersion) {
         return new L1VersionBody(value);
      }
      
      @Override
      protected void writeValue(BufferedWriter writer, L1Entry value) throws IOException {
         writer.write(String.valueOf(value.getValue().getValue()));
         writer.write("=[");
         writer.write(String.valueOf(value.getCreationVersion()));
         writer.write(",");
         writer.write(String.valueOf(value.getReadVersion()));
         writer.write("]");

      }

   }

   private class L1Entry {
      private final InternalCacheEntry value;
      private final EntryVersion creationVersion;
      private EntryVersion readVersion;
      private boolean invalid;

      public L1Entry(InternalGMUCacheEntry gmuCacheEntry) {
         this.value = gmuCacheEntry.getInternalCacheEntry();
         this.creationVersion = gmuCacheEntry.getCreationVersion();
         this.readVersion = gmuCacheEntry.getMaximumValidVersion();
         this.invalid = false;
      }

      public final InternalCacheEntry getValue() {
         return value;
      }

      public EntryVersion getCreationVersion() {
         return creationVersion;
      }

      public EntryVersion getReadVersion() {
         return readVersion;
      }

      public void setReadVersion(EntryVersion readVersion) {
         if (readVersion == null) {
            return;
         } else if (this.readVersion == null) {
            this.readVersion = readVersion;
            return;
         }
         this.readVersion = gmuVersionGenerator.mergeAndMax(readVersion, this.readVersion);
      }

      public boolean isInvalid() {
         return invalid;
      }

      public void setInvalid(boolean invalid) {
         this.invalid = invalid;
      }

      @Override
      public String toString() {
         return "L1Entry{" +
               "value=" + value +
               ", creationVersion=" + creationVersion +
               ", readVersion=" + readVersion +
               ", invalid=" + invalid +
               '}';
      }
   }

}
