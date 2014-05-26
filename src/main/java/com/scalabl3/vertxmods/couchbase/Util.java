package com.scalabl3.vertxmods.couchbase;

import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;

/**
 * Created by keghol on 5/25/14.
 */
public class Util {
    public static JsonObject createGenericResponse(Message<JsonObject> message) {
        JsonObject response = new JsonObject();
        response.putString("op", message.body().getString("op").toUpperCase());
        response.putString("key", message.body().getString("key"));
        response.putNumber("timestamp", System.currentTimeMillis());
        return response;
    }
}
