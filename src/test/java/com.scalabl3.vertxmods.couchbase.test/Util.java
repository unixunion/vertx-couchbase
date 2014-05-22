package com.scalabl3.vertxmods.couchbase.test;

import com.google.gson.Gson;
import org.vertx.java.core.json.JsonObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Type;

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

}
