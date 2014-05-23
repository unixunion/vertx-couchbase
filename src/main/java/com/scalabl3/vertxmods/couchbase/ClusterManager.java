package com.scalabl3.vertxmods.couchbase;

import org.vertx.java.busmods.BusModBase;
import org.vertx.java.core.Handler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.http.HttpClient;
import org.vertx.java.core.http.HttpClientRequest;
import org.vertx.java.core.http.HttpClientResponse;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.logging.Logger;

/**
 * Monitor couchbase buckets and stats
 *
 * Created by Kegan Holtzhausen on 21/05/14.
 */


public class ClusterManager extends BusModBase {

    private HttpClient client;
    JsonObject config;
    private Logger logger;

    private EventBus eventBus;

//    private AuthType authType; // use sasl
//    private BucketType bucketType; // memcached / couch
//    private Boolean flushEnabled; // allow flushing or not.
//    private String name; // name of bucket
//    private Number proxyPort; // port
//    private Number ramQuotaMB; // RAM
//    private Boolean replicaIndex; // replicate indexs, default: true
//    private Number replicaNumber; // number of replicas, default: 1
//    private String saslPassword; // sasl password if using sasl
//    private Number threadsNumber; // threads, default: 2
    private String host; // any host in the cluster
    private Integer port; // port, default: 8091
    private String address;


    // handlers
    private Handler<Message<JsonObject>> bucketHandler;
    // the handler to call when a response is received, which will answer the final message
    private Handler<HttpClientResponse> httpResponseHandler;


    private enum BucketType {
        memcached, couchbase
    }
    private enum AuthType {
        sasl, none
    }

    public void start() {
        super.start();

        eventBus = vertx.eventBus();
//        authType = Enum.valueOf(AuthType.class, getOptionalStringConfig("authType", "none"));
//        bucketType = Enum.valueOf(BucketType.class, getOptionalStringConfig("bucketType", "couchbase"));
//        flushEnabled = getOptionalBooleanConfig("flushEnabled", false);
//        name = getOptionalStringConfig("name", "default");
//        proxyPort = getOptionalIntConfig("proxyPort", 11711);
//        ramQuotaMB = getOptionalIntConfig("ramQuotaMB", 64);
//        replicaIndex = getOptionalBooleanConfig("replicaIndex", true);
//        replicaNumber = getOptionalIntConfig("replicaNumber", 1);
//        saslPassword = getOptionalStringConfig("saslPassword", "");
//        threadsNumber = getOptionalIntConfig("threadsNumber", 2);

        host = getOptionalStringConfig("couchbase.mgmthost", "localhost");
        port = getOptionalIntConfig("couchbase.mgmtport", 8091);
        address = getMandatoryStringConfig("address") + ".mgmt.bucket";

        container.logger().info("Connecting to: " + host + " port: " + port);

        client = vertx.createHttpClient().setPort(port).setHost(host);

        bucketHandler = new Handler<Message<JsonObject>>() {
            public void handle(Message<JsonObject> message) {
                getBuckets(message);
            }
        };
        eventBus.registerHandler(address, bucketHandler);

        container.logger().info("Deployed and listening on: " + address);

    }

    // do the last mile request and respond to the original message with couch-response data
    private void request(String url, final Message<JsonObject> message) {

        container.logger().info("url: " + url);
        container.logger().info("host: " + client.getHost());
        container.logger().info("port: " + client.getPort());

        HttpClientRequest request = client.get(url, new Handler<HttpClientResponse>() {
            public void handle(HttpClientResponse resp) {
                container.logger().info("Got a response: " + resp.statusCode());
                resp.bodyHandler(new Handler<Buffer>() {
                    public void handle(Buffer body) {
                        // The entire body has now been received
                        container.logger().info("Got Body: " + body);
                        message.reply(body);
                    }
                });

            }
        });
        makeRequest(request);

    };


    private void getBuckets(final Message<JsonObject> message ) {
        container.logger().info("getting buckets for message: " + message.body());
        String bucket = getMandatoryString("name", message);
        if (bucket.equals("all")) {
            request("/pools/default/buckets", message);
        } else {
            request("/pools/default/bucket/" + bucket, message);
        }
    };


    private void makeRequest(HttpClientRequest request, String body) {
        request.headers().add("Content-Length", String.valueOf(body.length()));
        request.write(body);
        makeRequest(request);
    }

    private void makeRequest(HttpClientRequest request) {
        container.logger().info("requesting..." + request.toString());
        request.headers().add("Accept", "application/json");
        request.end();
    }

}
