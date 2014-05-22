package org.vertx.mods;

import com.sun.org.apache.xpath.internal.operations.Bool;
import org.vertx.java.core.Handler;
import org.vertx.java.core.http.HttpClient;
import org.vertx.java.core.http.HttpClientRequest;
import org.vertx.java.core.http.HttpClientResponse;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.platform.Verticle;

import javax.naming.ConfigurationException;
import java.util.EnumSet;

/**
 * Create buckets and whatnot
 *
 * Created by Kegan Holtzhausen on 21/05/14.
 */


/*
    Class to manage buckets essentially
 */

public class ClusterManager extends Verticle implements Handler<HttpClientResponse> {

    private HttpClient client;
    JsonObject config;

    private AuthType authType; // use sasl
    private BucketType bucketType; // memcached / couch
    private Boolean flushEnabled; // allow flushing or not.
    private String name; // name of bucket
    private Number proxyPort; // port
    private Number ramQuotaMB; // RAM
    private Boolean replicaIndex; // replicate indexs, default: true
    private Number replicaNumber; // number of replicas, default: 1
    private String saslPassword; // sasl password if using sasl
    private Number threadsNumber; // threads, default: 2
    private String host; // any host in the cluster
    private Integer port; // port, default: 8091


    @Override
    public void handle(HttpClientResponse response) {
        if (response.statusCode() != 200) {
            throw new IllegalStateException("Invalid response, status: " + response.statusCode());
        }
        response.endHandler(new Handler() {

            @Override
            public void handle(Object event) {
                count++;
                if (count % 50 == 0) {
                    eb.send("rate-counter", count);
                    count = 0;
                }
                requestCredits++;
                makeRequest();
            }
        });
    }


    private enum BucketType {
        memcached, couchbase
    }
    private enum AuthType {
        sasl, none
    }

    public void start() {

        authType = Enum.valueOf(AuthType.class, container.config().getString("authType"));
        bucketType = Enum.valueOf(BucketType.class, container.config().getString("bucketType"));
        flushEnabled = container.config().getBoolean("flushEnabled", false);
        name = container.config().getString("name", "default");
        proxyPort = container.config().getNumber("proxyPort");
        ramQuotaMB = container.config().getNumber("ramQuotaMB");
        replicaIndex = container.config().getBoolean("replicaIndex", true);
        replicaNumber = container.config().getNumber("replicaNumber", 1);
        saslPassword = container.config().getString("saslPassword", "");
        threadsNumber = container.config().getInteger("threadsNumber", 2);
        host = container.config().getString("mgmthost", "localhost");
        port = container.config().getInteger("mgmtport", 8091);

        client = vertx.createHttpClient().setPort(port).setHost(host);

    }

    // Retrieves all bucket and bucket operations information from a cluster.
    public JsonObject getBucket(String bucket) {

        String url;

        if ( bucket == null ) {
            url =  "/pools/default/buckets";
        } else {
            url = "/pools/default/buckets/" + bucket;
        }

        HttpClientRequest request = client.get(url, this);
    };


    public Boolean createBucket() {
        String body = new JsonObject().putString("username", "user" + (int)(1000 * Math.random())).putString("password", "somepassword").toString();
        HttpClientRequest foo = client.post("/login", this);
        return true;
    }



}
