package com.deblox.couchperftest;

import org.junit.After;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
import org.junit.runners.Suite;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.AsyncResultHandler;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;
import org.vertx.testtools.TestVerticle;

import java.io.IOException;

import static org.vertx.testtools.VertxAssert.*;


/**
 * Created by keghol on 5/26/14.
 */

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TestPerformance extends TestVerticle {
    JsonObject config;
    EventBus eb;

    @Override
    public void start() {
        initialize();
        eb = vertx.eventBus();
        EventBus eb = vertx.eventBus();
        config = new JsonObject();
        config = com.scalabl3.vertxmods.couchbase.test.Util.loadConfig(this, "/conf-perftest.json");

        System.out.println("\n\n\nDeploy Worker Verticle Couchbase Async\n\n");

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

    public void createBucket(final Handler<Message> callback) {
        JsonObject request = new JsonObject()
                .putString("management", "CREATEBUCKET")
                .putString("name", "test")
                .putString("bucketType", "couchbase")
                .putNumber("memorySizeMB", 128)
                .putNumber("replicas", 0)
                .putString("authPassword", "")
                .putBoolean("flushEnabled", true)
                .putBoolean("ack", true);

        eb.send(config.getString("address"), request, new Handler<Message>() {
            @Override
            public void handle(Message event) {
                System.out.println(event.body());
//                assertEquals(true, Util.getSuccess(event));
                callback.handle(event);
            }
        });
    }

    public Boolean createDocuments() {
        System.out.println("Creating documents");
        JsonObject request;

        for(int i=0; i < 10000; i++) {
            // Create a new user object via User class
            User user = new User("user" + i, "somepassword");
            String user_string = Util.encode(user);

            request = new JsonObject().putString("op", "ADD")
                    .putString("key", "user" +i)
                    .putString("value", "badger badger badger")
                    .putNumber("expiry", 60)
                    .putBoolean("ack", true);

            vertx.eventBus().send(config.getString("address"), request);
        }
        System.out.println("Done");
        return true;
    }

    @Test
    public void simpleReadTest() {
        // create test documents
        createBucket(new Handler<Message>() {
            @Override
            public void handle(Message event) {
                Boolean cd = createDocuments();
                assertEquals(true, cd);
                container.deployVerticle("com.deblox.couchperftest.RateCounter", config);
                container.deployVerticle("com.deblox.couchperftest.SimpleReadTest", config, new AsyncResultHandler<String>() {
                    @Override
                    public void handle(AsyncResult<String> event) {
                        if (event.succeeded()) {
                            long timerID = vertx.setTimer(60000, new Handler<Long>() {
                                public void handle(Long timerID) {
                                    testComplete();
                                }
                            });
                        } else {
                            fail();
                        }
                    }
                });
            }
        });
    }


    @Test
    public void simpleWriteTest() {
        // create test documents
        createBucket(new Handler<Message>() {
            @Override
            public void handle(Message event) {
                container.deployVerticle("com.deblox.couchperftest.RateCounter", config);
                container.deployVerticle("com.deblox.couchperftest.SimpleWriteTest", config, new AsyncResultHandler<String>() {
                    @Override
                    public void handle(AsyncResult<String> event) {
                        if (event.succeeded()) {
                            long timerID = vertx.setTimer(60000, new Handler<Long>() {
                                public void handle(Long timerID) {
                                    testComplete();
                                }
                            });
                        } else {
                            fail();
                        }
                    }
                });
            }
        });
    }

    @Test
    public void ztearDown() throws IOException {
        System.out.println("@After tearDown");
        JsonObject request = new JsonObject()
                .putString("management", "FLUSHBUCKET")
                .putString("name", config.getString("couchbase.bucket"))
                .putBoolean("ack", true);
        eb.send(config.getString("address"), request);

//        request = new JsonObject()
//                .putString("management", "DELETEBUCKET")
//                .putString("name", config.getString("couchbase.bucket"))
//                .putBoolean("ack", true);
//        eb.send(config.getString("address"), request);

        testComplete();

    }

}
