package org.infinispan.interceptors.gmu;

import org.infinispan.container.entries.gmu.InternalGMUCacheEntry;
import org.infinispan.context.impl.TxInvocationContext;

/**
 * @author Nuno Diegues
 * @since 5.2
 */
public class SSIEntryWrappingInterceptor extends GMUEntryWrappingInterceptor {

   @Override
   protected void shouldEarlyAbort(TxInvocationContext txInvocationContext, InternalGMUCacheEntry internalGMUCacheEntry) {
      // empty on purpose, SSI never aborts because of a single stale read
   }
   
}
