package com.scalabl3.vertxmods.couchbase.async;

import com.couchbase.client.CouchbaseClient;
import org.vertx.java.core.json.JsonArray;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Collection;
import java.util.concurrent.Callable;

public class StatusCallable implements Callable<JsonArray[]> {

    private CouchbaseClient cbClient;

    public StatusCallable(CouchbaseClient cbClient) {
        this.cbClient = cbClient;
    }

    @Override
    public JsonArray[] call() throws Exception {
        Collection<SocketAddress> available = cbClient.getAvailableServers();
        Collection<SocketAddress> unavailable = cbClient.getUnavailableServers();
        JsonArray aArr = new JsonArray();
        for (SocketAddress sa : available) {
            aArr.addString(((InetSocketAddress) sa).getHostString() + ":" + ((InetSocketAddress) sa).getPort());
        }
        JsonArray uArr = new JsonArray();
        for (SocketAddress sa : unavailable) {
            uArr.addString(((InetSocketAddress) sa).getHostString() + ":" + ((InetSocketAddress) sa).getPort());
        }
        return new JsonArray[]{aArr, uArr};
    }
}
