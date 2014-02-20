package com.scalabl3.vertxmods.couchbase.async;

import com.couchbase.client.CouchbaseClient;

import java.net.SocketAddress;
import java.util.Map;
import java.util.concurrent.Callable;

public class ClusterStatsCallable implements Callable<Map<SocketAddress, Map<String, String>>> {

    private CouchbaseClient cbClient;

    public ClusterStatsCallable(CouchbaseClient cbClient) {
        this.cbClient = cbClient;
    }

    @Override
    public Map<SocketAddress, Map<String, String>> call() throws Exception {
        return cbClient.getStats();
    }
}
