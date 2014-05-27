package com.scalabl3.vertxmods.couchbase;

import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

/**
 * Created by keghol on 5/25/14.
 */
public class Util {
    public static JsonObject createGenericResponse(Message<JsonObject> message) {
        JsonObject response = new JsonObject();
        response.putString("op", message.body().getString("op").toUpperCase());
        response.putString("key", message.body().getString("key"));
        response.putArray("keys", message.body().getArray("keys"));
        response.putNumber("timestamp", System.currentTimeMillis());
        return response;
    }

    public static String voidNull(String s) {
        return s == null ? "" : s;
    }

    public static void checkTimeout(Future f) throws TimeoutException {
        if(f == null) {
            throw new TimeoutException();
        }
    }

    public static String getKey(Message<JsonObject> message) throws Exception {
        String key = voidNull(message.body().getString("key"));
        if (key.isEmpty()) {
            throw new Exception("Missing mandatory non-empty field 'key'");
        }
        return key;
    }

    public static Object getValue(Message<JsonObject> message) throws Exception {
        Object value = message.body().getValue("value");
//                getValue(message);
        if (value == null) {
            throw new Exception("Missing mandatory non-empty field 'value'");
        }
        return value;
    }



    public static JsonObject parseForJson(JsonObject jsonObject, String key, Object value) throws Exception {
        if (value != null) {
            // not serializable in current version of vert.x
            /*
            * if(value instanceof JsonArray) jsonObject.putArray("value", (JsonArray) value); else if(value instanceof JsonObject) jsonObject.putObject("value", (JsonObject) value); else
            */

            if (value instanceof byte[]) {
                jsonObject.putBinary(key, (byte[]) value);
            } else if (value instanceof Boolean) {
                jsonObject.putBoolean(key, (Boolean) value);
            } else if (value instanceof Number) {
                jsonObject.putNumber(key, (Number) value);
            } else if (value instanceof String) {
                jsonObject.putString(key, (String) value);
            } else {
                System.out.println(value);
                throw new Exception("unsupported object type: " + value.getClass());
            }
        }
        return jsonObject;
    }

    public static ExecutorService syncExecutor = Executors.newFixedThreadPool(2);


}



