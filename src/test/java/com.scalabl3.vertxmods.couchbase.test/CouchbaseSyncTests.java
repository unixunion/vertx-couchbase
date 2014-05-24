package com.scalabl3.vertxmods.couchbase.test;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.vertx.groovy.core.eventbus.EventBus;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.AsyncResultHandler;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;
import org.vertx.testtools.TestVerticle;

import java.io.IOException;
import static org.vertx.testtools.VertxAssert.*;

public class CouchbaseSyncTests extends TestVerticle{

    private static EventBus eb;
    private JsonObject config;

    public void start() {
        initialize();

        org.vertx.java.core.eventbus.EventBus eb = vertx.eventBus();
        config = new JsonObject();

        config.putString("address", "vertx.couchbase.sync");
        config.putString("couchbase.nodelist", "localhost:8091");
        config.putString("couchbase.bucket", "default");
        config.putString("couchbase.bucket.password", "");
        config.putNumber("couchbase.num.clients", 1);
        config.putBoolean("async_mode", false);

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

    private void println(String string) {
        System.out.println(string);
    }


    @Test
    public void get_design_document() {
        JsonObject request = new JsonObject().putString("op", "GETDESIGNDOC")
                .putString("design_doc", "dev_test")
                .putBoolean("ack", true);

        vertx.eventBus().send(config.getString("address"), request, new Handler<Message<JsonObject>>() {

            @Override
            public void handle(final Message<JsonObject> reply) {
//                System.out.println("Got Document : " + reply.body());
                assertEquals(true, Util.getResponse(reply).getBoolean("exists"));
                testComplete();
            }
        });
    }

    @Before
    public void setUp() {
        this.println("@Before setUp");
        //EventBus eb = vertx.eventBus();
    }

    @After
    public void tearDown() throws IOException {
        this.println("@After tearDown");

    }

    @Test
    public void test1() {
        this.println("@Test test1()");
        testComplete();
    }

    @Test
    public void test2() {
        this.println("@Test test2()");
        testComplete();
    }
}

