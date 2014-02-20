import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.deploy.Verticle;

import java.util.HashMap;

/**
 * Created with IntelliJ IDEA.
 * User: jasdeep
 * Date: 6/8/13
 * Time: 9:03 AM
 * To change this template use File | Settings | File Templates.
 */
public class Main extends Verticle {

    String address;

    @Override
    public void start() throws Exception {


        EventBus eb = vertx.eventBus();
        address = "vertx.couchbase.async";

        JsonObject config = new JsonObject();
        config.putString("address", address);
        config.putString("couchbase.nodelist", "localhost:8091");
        config.putString("couchbase.bucket", "default");
        config.putString("couchbase.bucket.password", "");
        config.putNumber("couchbase.timeout.ms", 10000);
        config.putNumber("couchbase.tasks.check.ms", 500);
        config.putNumber("couchbase.num.clients", 1);

        System.out.println("\n\n\nDeploy Verticle Couchbase Async\n\n");

        container.deployVerticle("com.scalabl3.vertxmods.couchbase.async.CouchbaseEventBusAsync", config, 1, new Handler<String>() {
            @Override
            public void handle(String s) {

            }
        });

        address = "vertx.couchbase.sync";

        config.putString("address", address);
        config.putString("couchbase.nodelist", "localhost:8091");
        config.putString("couchbase.bucket", "default");
        config.putString("couchbase.bucket.password", "");
        config.putNumber("couchbase.num.clients", 1);

        System.out.println("\n\n\nDeploy Worker Verticle Couchbase Sync\n\n");

        container.deployWorkerVerticle("com.scalabl3.vertxmods.couchbase.sync.CouchbaseEventBusSync", config, 1, new Handler<String>() {
            @Override
            public void handle(String s) {


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
                System.out.println("received: \n" +message.body.encode());
            }
        };
        vertx.eventBus().send(address, notif, replyHandler);
    }
}
