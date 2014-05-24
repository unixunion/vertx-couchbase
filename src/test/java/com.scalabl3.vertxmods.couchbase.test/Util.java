package com.scalabl3.vertxmods.couchbase.test;

import com.google.gson.Gson;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;

import java.lang.reflect.Type;
import java.util.NoSuchElementException;

/**
 * Created by marzubus on 18/05/14.
 */

public class Util {

    static public String encode(Object val) {
        Gson gson = new Gson();
        return gson.toJson(val);
    }

    static public Object decode(String val, Class<?> typeOfT) {
        Gson gson = new Gson();
        return gson.fromJson(val, typeOfT);
    }

    static public Object decode(String val, Type typeOfT) {
        Gson gson = new Gson();
        return gson.fromJson(val, typeOfT);
    }

    // return the response jsonobject portion of the message
    static public JsonObject getResponse(Message message) {
        try {
            return new JsonObject(message.body().toString()).getObject("response");
        } catch (Exception e) {
            e.printStackTrace();
            throw new NoSuchElementException("No response object in message");
        }
    }

    // get the message's success boolean
    static public Boolean getSuccess(Message message) {
        try {
            return new JsonObject(message.body().toString()).getBoolean("success");
        } catch (Exception e) {
            throw  new NoSuchElementException("No success boolean in message");
        }
    }

}
