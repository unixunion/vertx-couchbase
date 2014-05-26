package com.scalabl3.vertxmods.couchbase.test;

import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.AsyncResultHandler;
import org.vertx.java.core.Handler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;
import org.vertx.testtools.TestVerticle;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import static org.vertx.testtools.VertxAssert.*;

public class ClusterManagerTests extends TestVerticle {

    EventBus eb;
    JsonObject config;
    DefaultPrettyPrinter pp;
    ObjectMapper mapper;
    String address;

    @Override
    public void start() {
        initialize();

        eb = vertx.eventBus();
        config = Util.loadConfig(this, "/conf-async.json");
        address = config.getString("address"); // save for tests

        // prettyprinter
        pp = new DefaultPrettyPrinter();
        pp.indentArraysWith(new DefaultPrettyPrinter.Lf2SpacesIndenter());
        mapper = new ObjectMapper();

        System.out.println("\n\n\nDeploying Mod Couchbase\n\n");

        container.deployVerticle("com.scalabl3.vertxmods.couchbase.Boot", config, new AsyncResultHandler<String>() {

            @Override
            public void handle(AsyncResult<String> asyncResult) {

                // Deployment is asynchronous and this this handler will be called when it's complete (or failed)
                if (asyncResult.failed()) {
                    container.logger().error(asyncResult.cause());
                }

                assertTrue(asyncResult.succeeded());
                assertNotNull("deploymentID should not be null", asyncResult.result());

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                // If deployed correctly then start the tests!
                startTests();
            }
        });

    }

    @Test
    public void createBuckets() {
        JsonObject request = new JsonObject()
                .putString("management", "CREATEBUCKET")
                .putString("name", "test")
                .putString("bucketType", "couchbase")
                .putNumber("memorySizeMB", 128)
                .putNumber("replicas", 0)
                .putString("authPassword", "")
                .putBoolean("flushEnabled", true)
                .putBoolean("ack", true);

        container.logger().info("sending message " + request.toString() + " to address: " + address);

        eb.send(address, request, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> event) {
                container.logger().info("response: " + event.body());
                assertTrue(getSuccess(event));                assertTrue(getSuccess(event));
                testComplete();
            };
        });
    }

    @Test
    public void z_deleteBuckets() {
        JsonObject request = new JsonObject()
                .putString("management", "DELETEBUCKET")
                .putString("name", "test")
                .putBoolean("ack", true);

        container.logger().info("sending message " + request.toString() + " to address: " + address);

        eb.send(address, request, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> event) {
                container.logger().info("response: " + event.body());
                assertTrue(getSuccess(event));
                testComplete();
            };
        });
    }


    @Test
    public void createPortBuckets() {
        JsonObject request = new JsonObject()
                .putString("management", "CREATEPORTBUCKET")
                .putString("name", "test_port")
                .putString("bucketType", "couchbase")
                .putNumber("memorySizeMB", 128)
                .putNumber("replicas", 0)
                .putNumber("port", 30000)
                .putBoolean("flushEnabled", true)
                .putBoolean("ack", true);

        container.logger().info("sending message " + request.toString() + " to address: " + address);

        eb.send(address, request, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> event) {
                container.logger().info("response: " + event.body());
                assertTrue(getSuccess(event));
                testComplete();
            };
        });
    }

    @Test
    public void z_deletePortBucket() {
        JsonObject request = new JsonObject()
                .putString("management", "DELETEBUCKET")
                .putString("name", "test_port")
                .putBoolean("ack", true);

        container.logger().info("sending message " + request.toString() + " to address: " + address);

        eb.send(address, request, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> event) {
                container.logger().info("response: " + event.body());
                assertTrue(getSuccess(event));
                testComplete();
            };
        });
    }


    @Test
    public void listBuckets() {
        JsonObject request = new JsonObject()
                .putString("management", "LISTBUCKETS")
                .putBoolean("ack", true);

        container.logger().info("sending message " + request.toString() + " to address: " + address);

        eb.send(address, request, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> event) {
                container.logger().info("response: " + event.body());
                assertTrue(getSuccess(event));
                testComplete();
            };
        });
    }

    // get the response success boolean out of the event
    public Boolean getSuccess(Message<JsonObject> event) {
        return event.body().getObject("response").getBoolean("success");
    }


}
