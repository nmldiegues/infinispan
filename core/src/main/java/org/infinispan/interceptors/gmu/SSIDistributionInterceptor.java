package org.infinispan.interceptors.gmu;

import static org.infinispan.transaction.gmu.GMUHelper.joinAndSetTransactionVersionAndFlags;

import java.util.Collection;
import java.util.Map;

import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

public class SSIDistributionInterceptor extends GMUDistributionInterceptor {

   private static final Log log = LogFactory.getLog(SSIDistributionInterceptor.class);
   
   @Override
   protected void prepareOnAffectedNodes(TxInvocationContext ctx, PrepareCommand command,
                                         Collection<Address> recipients, boolean sync) {
      Map<Address, Response> responses = rpcManager.invokeRemotely(recipients, command, true, true, false);
      log.debugf("prepare command for transaction %s is sent. responses are: %s",
                 command.getGlobalTransaction().prettyPrint(), responses.toString());

      joinAndSetTransactionVersionAndFlags(responses.values(), ctx, versionGenerator);
   }
   
}
