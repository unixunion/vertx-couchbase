package com.deblox.couchperftest;

import com.google.gson.Gson;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
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
//    static public Boolean getSuccess(Message message) {
//        try {
//            return new JsonObject(message.body().toString()).getBoolean("success");
//        } catch (Exception e) {
//            throw  new NoSuchElementException("No success boolean in message");
//        }
//    }

    static public JsonObject loadConfig(Object o, String file) {

        try (InputStream stream = o.getClass().getResourceAsStream(file)) {
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

    // get the response success boolean out of the event
    public static Boolean getSuccess(Message<JsonObject> event) {
        return event.body().getObject("response").getBoolean("success");
    }


}
