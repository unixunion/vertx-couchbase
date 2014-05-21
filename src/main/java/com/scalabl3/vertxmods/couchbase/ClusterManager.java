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
    private String name;
    private Number proxyPort;
    private Number ramQuotaMB;
    private Boolean replicaIndex;
    private Number replicaNumber;
    private String saslPassword;
    private Number threadsNumber;
    private String host;
    private Integer port;

    @Override
    public void handle(HttpClientResponse event) {
        
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
        name = container.config().getString("name", "users");
        proxyPort = container.config().getNumber("proxyPort");
        ramQuotaMB = container.config().getNumber("ramQuotaMB");
        replicaIndex = container.config().getBoolean("replicaIndex", true);
        replicaNumber = container.config().getNumber("replicaNumber", 1);
        saslPassword = container.config().getString("saslPassword", "");
        threadsNumber = container.config().getInteger("threadsNumber", 2);
        host = container.config().getString("mgmthost", "localhost");
        port = container.config().getInteger("mgmtport", 8080);


        client = vertx.createHttpClient().setPort(port).setHost(host);

    }


    public Boolean createBucket() {
        String body = new JsonObject().putString("username", "user" + (int)(1000 * Math.random())).putString("password", "somepassword").toString();
        HttpClientRequest foo = client.post("/login", this);
        return true;
    }





}
