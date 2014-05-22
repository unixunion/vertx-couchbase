package com.scalabl3.vertxmods.couchbase;

import groovyjarjarantlr.debug.MessageAdapter;
import org.vertx.java.busmods.BusModBase;
import org.vertx.java.core.Handler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.eventbus.Message;
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

public class ClusterManager extends BusModBase {

    private HttpClient client;
    JsonObject config;

    private EventBus eventBus;

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
    private String address;


    // handlers
    private Handler<Message<JsonObject>> bucketHandler;
    // the handler to call when a response is received, which will answer the final message
    private Handler<HttpClientResponse> httpResponseHandler;

//    public void handle(HttpClientResponse response) {
//
//        if (response.statusCode() != 200) {
//            throw new IllegalStateException("Invalid response, status: " + response.statusCode());
//        }
//        response.bodyHandler(new Handler<Buffer>() {
//            public void handle(Buffer event) {
//                container.logger().debug(event.toString());
//            }
//        });
//
//    }


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
        client = vertx.createHttpClient().setPort(port).setHost(host);

        bucketHandler = new Handler<Message<JsonObject>>() {
            public void handle(Message<JsonObject> message) {
                getBuckets(message);
            }
        };
        eb.registerHandler(address, bucketHandler);

        container.logger().info("\n\n\nDeployed and listening on: " + address);

    }

    // do the last mile request and respond to the original message with couch-response data
    private void request(String url, final Message<JsonObject> message) {

        httpResponseHandler = new Handler<HttpClientResponse>() {
            @Override
            public void handle(HttpClientResponse event) {
                event.bodyHandler(new Handler<Buffer>() {
                    @Override
                    public void handle(Buffer event) {
                        message.reply(event);
                    }
                });
            }
        };

        HttpClientRequest request = client.get(url, httpResponseHandler);
        makeRequest(request);

    }

    // returns all buckets
    private void getBuckets(final Message<JsonObject> message) {
        container.logger().debug("getBuckets called with message: " + message.body().toString());
        getBucket(null, message);
    }

    // return the specified bucket
    private void getBucket(String bucket, final Message<JsonObject> message) {

        String url = null;
        String body = null;

        if ( bucket == null ) {
            url =  "/pools/default/buckets";
        } else {
            url = "/pools/default/buckets/" + bucket;
        }

        request(url, message );
    };


    private void makeRequest(HttpClientRequest request, String body) {
        request.headers().add("Content-Length", String.valueOf(body.length()));
        request.write(body);
        makeRequest(request);
    }

    private void makeRequest(HttpClientRequest request) {
        request.end();
    }

}
