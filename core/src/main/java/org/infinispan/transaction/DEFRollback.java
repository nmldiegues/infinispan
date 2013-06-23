package org.infinispan.transaction;

import java.io.Serializable;
import java.util.Set;

import org.infinispan.Cache;
import org.infinispan.distexec.DistributedCallable;
import org.infinispan.transaction.xa.GlobalTransaction;

public class DEFRollback implements DistributedCallable, Serializable {

    private GlobalTransaction tx;
    
    public DEFRollback(GlobalTransaction tx) {
	this.tx = tx;
    }

    @Override
    public Object call() throws Exception {
	// TODO resume, prepare, suspend, return result
	return null;
    }

    @Override
    public void setEnvironment(Cache cache, Set inputKeys) {
    }

}