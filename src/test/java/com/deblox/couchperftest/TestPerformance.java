package com.deblox.couchperftest;

import org.junit.Test;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.AsyncResultHandler;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;
import org.vertx.testtools.TestVerticle;

import static org.vertx.testtools.VertxAssert.*;


/**
 * Created by keghol on 5/26/14.
 */

public class TestPerformance extends TestVerticle {
    JsonObject config;

    @Override
    public void start() {
        initialize();

        EventBus eb = vertx.eventBus();
        config = new JsonObject();
        config = com.scalabl3.vertxmods.couchbase.test.Util.loadConfig(this, "/conf-async.json");

        System.out.println("\n\n\nDeploy Worker Verticle Couchbase Async\n\n");

        container.deployWorkerVerticle("com.scalabl3.vertxmods.couchbase.Boot", config, 1, true, new AsyncResultHandler<String>() {

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
    public void simpleReadTest() {
        // create test documents

        System.out.println("Creating user documents");
        for(int i=0; i < 10000; i++) {
            // Create a new user object via User class
            User user = new User("user" + i, "somepassword");
            String user_string = Util.encode(user);

            JsonObject request = new JsonObject().putString("op", "ADD")
                    .putString("key", "user" +i)
                    .putString("value", user_string)
                    .putNumber("expiry", 86400)
                    .putBoolean("ack", true);

            vertx.eventBus().send(config.getString("address"), request);
        }
        System.out.println("Done, starting read test");

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

}
