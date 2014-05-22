import org.junit.Test;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.AsyncResultHandler;
import org.vertx.java.core.Handler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;
import com.scalabl3.vertxmods.couchbase.test.Util;
import org.vertx.testtools.TestVerticle;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import static org.vertx.testtools.VertxAssert.assertNotNull;
import static org.vertx.testtools.VertxAssert.assertTrue;

public class ClusterManagerTests extends TestVerticle {

    EventBus eb;
    JsonObject config;

    @Override
    public void start() {
        initialize();

        eb = vertx.eventBus();
        config = loadConfig("/conf.json");

        System.out.println("\n\n\nDeploy Couchbase Manager\n\n");

        container.deployWorkerVerticle("com.scalabl3.vertxmods.couchbase.ClusterManager", config, 1, true, new AsyncResultHandler<String>() {

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
    public void getBuckets() {
        JsonObject request = new JsonObject();
        request.putString("name", null);

        String address = config.getString("address") + ".mgmt.bucket";
        container.logger().info("sending to address: " + address);

        eb.send(address, request, new Handler<Message<Buffer>>() {
//            @Override
//            public void handle(Message<JsonObject> event) {
//                container.logger().info("got reply event: " + event.body().toString());
//            }

            @Override
            public void handle(Message<Buffer> event) {
                final String body = event.getString(0,event.length());
                container.logger().info("response: " + body);
            }
        });

    }


    JsonObject loadConfig(String file) {
        System.out.println(System.getProperty("java.class.path"));
        try (InputStream stream = this.getClass().getResourceAsStream(file)) {
            StringBuilder sb = new StringBuilder();
            BufferedReader reader = new BufferedReader(new InputStreamReader(stream, "UTF-8"));

            String line = reader.readLine();
            while (line != null) {
                sb.append(line).append('\n');
                line = reader.readLine();
                System.out.println(line);
            }

            return new JsonObject(sb.toString());

        } catch (IOException e) {
            e.printStackTrace();
            return new JsonObject();
        }

    }

}
