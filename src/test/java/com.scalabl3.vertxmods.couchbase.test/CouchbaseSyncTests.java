package com.scalabl3.vertxmods.couchbase.test;

import com.couchbase.client.protocol.views.DesignDocument;
import com.couchbase.client.protocol.views.ViewDesign;
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
    public void create_design_document() {

        ViewDesign view1 = new ViewDesign(
                "view1",
                "function(a, b) {}"
        );

        DesignDocument dd = new DesignDocument("testtest");
        dd.setView(view1);

        JsonObject request = new JsonObject().putString("op", "CREATEDESIGNDOC")
                .putString("name", "dev_test1")
                .putString("value", dd.toJson())
                .putBoolean("ack", true);

        System.out.println(request.toString());

        vertx.eventBus().send(config.getString("address"), request, new Handler<Message<JsonObject>>() {

            @Override
            public void handle(final Message<JsonObject> reply) {
                assertEquals(true, Util.getResponse(reply).getBoolean("success"));
                System.out.println("Got Response : " + reply.body());
                testComplete();
            }
        });

    }

    @Test
    public void create_design_document_error() {

        ViewDesign view1 = new ViewDesign(
                "view1",
                "function(a, b) {}"
        );

        DesignDocument dd = new DesignDocument("testtest");
        dd.setView(view1);

        JsonObject request = new JsonObject().putString("op", "CREATEDESIGNDOC")
                .putString("name", "dev_test1")
                .putString("value", "error rorororro")
                .putBoolean("ack", true);

        System.out.println(request.toString());

        vertx.eventBus().send(config.getString("address"), request, new Handler<Message<JsonObject>>() {

            @Override
            public void handle(final Message<JsonObject> reply) {
                assertEquals(false, Util.getResponse(reply).getBoolean("success"));
                System.out.println("Got Response : " + reply.body());
                testComplete();
            }
        });

    }

    @Test
    public void get_design_document() {
        JsonObject request = new JsonObject().putString("op", "GETDESIGNDOC")
                .putString("name", "dev_test")
                .putBoolean("ack", true);

        System.out.println(request.toString());

        vertx.eventBus().send(config.getString("address"), request, new Handler<Message<JsonObject>>() {

            @Override
            public void handle(final Message<JsonObject> reply) {
                System.out.println("Got Document : " + reply.body());
                assertEquals(true, Util.getResponse(reply).getBoolean("exists"));
                testComplete();
            }
        });
    }


    @Test
    public void get_missing_design_document() {
        JsonObject request = new JsonObject().putString("op", "GETDESIGNDOC")
                .putString("design_doc", "dev_testdsds")
                .putBoolean("ack", true);

        System.out.println(request.toString());

        vertx.eventBus().send(config.getString("address"), request, new Handler<Message<JsonObject>>() {

            @Override
            public void handle(final Message<JsonObject> reply) {
//                System.out.println("Got Document : " + reply.body());
                assertEquals("error", reply.body().getString("status"));
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

