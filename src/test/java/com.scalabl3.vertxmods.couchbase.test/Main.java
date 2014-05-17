import org.junit.Test;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.AsyncResultHandler;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.vertx.testtools.TestVerticle;
import static org.vertx.testtools.VertxAssert.*;

import java.util.HashMap;

/**
 * Created with IntelliJ IDEA.
 * User: jasdeep
 * Date: 6/8/13
 * Time: 9:03 AM
 * To change this template use File | Settings | File Templates.
 */
public class Main extends TestVerticle {

    String address;
    JsonObject config;

    @Override
    public void start() {
        initialize();

        EventBus eb = vertx.eventBus();
        config = new JsonObject();
//        address = "vertx.couchbase.async";
//
//
//        config.putString("address", address);
//        config.putString("couchbase.nodelist", "localhost:8091");
//        config.putString("couchbase.bucket", "default");
//        config.putString("couchbase.bucket.password", "");
//        config.putNumber("couchbase.timeout.ms", 10000);
//        config.putNumber("couchbase.tasks.check.ms", 500);
//        config.putNumber("couchbase.num.clients", 1);
//
//        System.out.println("\n\n\nDeploy Verticle Couchbase Async\n\n");
//
//        container.deployVerticle("com.scalabl3.vertxmods.couchbase.async.CouchbaseEventBusAsync", config, 1, new Handler<String>() {
//            @Override
//            public void handle(String s) {
//
//            }
//        });

        config.putString("address", "vertx.couchbase.sync");
        config.putString("couchbase.nodelist", "localhost:8091");
        config.putString("couchbase.bucket", "ivault");
        config.putString("couchbase.bucket.password", "");
        config.putNumber("couchbase.num.clients", 1);

        System.out.println("\n\n\nDeploy Worker Verticle Couchbase Sync\n\n");

//        container.deployWorkerVerticle("com.scalabl3.vertxmods.couchbase.sync.CouchbaseEventBusSync", config, 1, true, new AsyncResultHandler<String>() {

        container.deployWorkerVerticle("com.scalabl3.vertxmods.couchbase.sync.CouchbaseEventBusSync", config, 1, true, new AsyncResultHandler<String>() {

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
    public void foo() {

        JsonObject request = new JsonObject().putString("op", "ADD")
                .putString("key", "1")
                .putString("value", "data goes here")
                .putBoolean("ack", true);

        container.logger().info("sending message to address: " + config.getString("address"));

        vertx.eventBus().send(config.getString("address"), request, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                try {
                    System.out.println("Response: " + reply.body());
                    JsonObject body = reply.body();
                    assertNotNull(body.toString());
                    testComplete();
                } catch (Exception e) {
                    e.printStackTrace();
                    throw e;
                }
            }
        });
    }

    public void act(HashMap<String, Object> cmd)
    {
        if(cmd == null)
            return;

        JsonObject notif = new JsonObject();

        for(String key : cmd.keySet())
        {
            Object value = cmd.get(key);

            if(value != null)
            {
                if(value instanceof byte[])
                    notif.putBinary(key, (byte[]) value);
                else if(value instanceof Boolean)
                    notif.putBoolean(key, (Boolean) value);
                else if(value instanceof Number)
                    notif.putNumber(key, (Number) value);
                else if(value instanceof String)
                    notif.putString(key, (String) value);
                else if(value instanceof JsonArray)
                    notif.putArray(key, (JsonArray) value);
            }
        }
        System.out.println("sent: \n" + notif.encode());
        push(notif);
    }

    private void push(JsonObject notif)
    {
        Handler<Message<JsonObject>> replyHandler = new Handler<Message<JsonObject>>()
        {
            public void handle(Message<JsonObject> message)
            {
                System.out.println("received: \n" +message.body().encode());
            }
        };
        vertx.eventBus().send(address, notif, replyHandler);
    }
}
