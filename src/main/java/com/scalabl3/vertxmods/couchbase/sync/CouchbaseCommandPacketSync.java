package com.scalabl3.vertxmods.couchbase.sync;

import com.couchbase.client.CouchbaseClient;
import com.couchbase.client.internal.HttpFuture;
import com.couchbase.client.protocol.views.DesignDocument;
import com.couchbase.client.protocol.views.Query;
import com.couchbase.client.protocol.views.View;
import com.couchbase.client.protocol.views.ViewResponse;
import com.scalabl3.vertxmods.couchbase.Util;
import net.spy.memcached.CASResponse;
import net.spy.memcached.CASValue;
import net.spy.memcached.PersistTo;
import net.spy.memcached.ReplicateTo;
import net.spy.memcached.internal.OperationFuture;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

//import com.couchbase.client.internal.HttpFuture;

@SuppressWarnings("unchecked")
public enum CouchbaseCommandPacketSync {

    /*
    Delete Design Doc
    */
    DELETEDESIGNDOC() {
        @Override
        public JsonObject operation(CouchbaseClient cb, Message<JsonObject> message) throws Exception {
            String name = message.body().getString("name");
            JsonObject result = new JsonObject();
            result.putBoolean("success", cb.deleteDesignDoc(name));
            return result;
        }

        @Override
        public JsonObject buildResponse(Message<JsonObject> message, JsonObject result, boolean returnAcknowledgement) throws Exception {

            if(!returnAcknowledgement) {
                return null;
            }

            JsonObject response = Util.createGenericResponse(message);
            response.putBoolean("success", result.getBoolean("success"));
            return response;
        }
    },

    /*
    Create Design Doc
    */
    CREATEDESIGNDOC() {
        @Override
        public JsonObject operation(CouchbaseClient cb, Message<JsonObject> message) throws Exception {
            String name = message.body().getString("name");
            String value = message.body().getString("value");
            HttpFuture<Boolean> f = cb.asyncCreateDesignDoc(name, value);
            return new JsonObject().putBoolean("value", f.get());
        }

        @Override
        public JsonObject buildResponse(Message<JsonObject> message, JsonObject result, boolean returnAcknowledgement) throws Exception {

            if(!returnAcknowledgement) {
                return null;
            }

            JsonObject response = Util.createGenericResponse(message);
            response.putBoolean("success", result.getBoolean("value"));
            return response;
        }
    },

    /*
    get design doc
     */
    GETDESIGNDOC() {
        @Override
        public JsonObject operation(CouchbaseClient cb, Message<JsonObject> message) throws Exception {
            String design_doc = message.body().getString("name");
//            System.out.println("sync GETDESIGNDOC: getting " + design_doc);
            DesignDocument result = cb.getDesignDoc(design_doc);
//            System.out.println(result.toJson());
            return new JsonObject(result.toJson());
        }

        @Override
        public JsonObject buildResponse(Message<JsonObject> message, JsonObject result, boolean returnAcknowledgement) throws Exception {

            JsonObject response = Util.createGenericResponse(message);
            JsonObject data = new JsonObject();
            // if we made it this far, it exists.
            response.putBoolean("exists", true);
            response.putObject("data", result);
            response.putBoolean("success", true);

            return response;
        }
    },
    /*
    * Atomic Counter Operations
    * INCR, DECR
    *
    */

    QUERY() {
        // views and whatnot
            @Override
            public JsonObject operation(CouchbaseClient cb, Message<JsonObject> message) throws Exception {
//                System.out.println("query called with message: " + message.body().toString());
                String design_doc = message.body().getString("design_doc");
                String view_name = message.body().getString("view_name");
                String key = message.body().getString("key", null);
                JsonArray keys = message.body().getArray("keys", null);
                String start_key = message.body().getString("start_key");
                String end_key = message.body().getString("end_key");
                Boolean include_docs = message.body().getBoolean("include_docs", true);

                View view = cb.getView(design_doc, view_name);
                Query query = new Query();

                // was key sent
                if (key != null) {
//                    System.out.println("key set to: " + key);
                    query.setKey(key);
                }

                // was keys sent
                if (keys != null) {
//                    System.out.println("keys set to: " + keys.toString());
                    query.setKeys(String.valueOf(keys));
                }

                query.setIncludeDocs(include_docs);
                JsonObject result = new JsonObject();
                ViewResponse response = cb.query(view, query);
                result.putArray("result", new JsonArray(response.getMap().values().toArray()));

//                System.out.println("Response type: " + response.getClass().toString());
//                System.out.println("Response String: " + response.toString());




//                JsonArray ja = new JsonArray();
//                for (ViewRow row : response) {
//                    ja.add(row.getDocument());
//                }
//                result.putArray("result", ja);
//                System.out.println("Response from DB: " + response.toString());

//                JsonObject data = new JsonObject();
//                data = Util.parseForJson(data, "value", response.toString());
//                System.out.println("Util.parseForJson: " + data.toString());

//                Object value = Util.getValue(message);
//                System.out.println("Util.getValue: " + value.toString());



                result.putBoolean("success", true);
//                if (include_docs) {
//                    result.putArray("result", new JsonArray(response.getMap().values().toArray()));
//
//                } else {
//                    for (ViewRow row : response) {
//                        result.putArray("result", new JsonArray(row.getValue()));
//                    }
//
//                }


                return result;

            }

            @Override
            public JsonObject buildResponse(Message<JsonObject> message, JsonObject result, boolean returnAcknowledgement) throws Exception {

                if(!returnAcknowledgement) {
                    return null;
                }

                JsonObject response = Util.createGenericResponse(message);

                if (result.getBoolean("success")) {
                    response.putBoolean("success", true);
                } else {
                    response.putBoolean("success", false);
                }

                response.putObject("response", result);
                return response;
            }
    },

    INCR() {
        @Override
        public JsonObject operation(CouchbaseClient cb, Message<JsonObject> message) throws Exception {

            String key = Util.getKey(message);
            Number by = message.body().getNumber("by") == null? 1 : message.body().getNumber("by");

            JsonObject result = new JsonObject();

            if (Util.isInteger(by)) {
                result.putNumber("result", cb.incr(key, (int)by));
            } else {
                result.putNumber("result", cb.incr(key, (long)by));
            }

            return result;

        }

        @Override
        public JsonObject buildResponse(Message<JsonObject> message, JsonObject result, boolean returnAcknowledgement) throws Exception {

            if(!returnAcknowledgement) {
                return null;
            }

            JsonObject response = Util.createGenericResponse(message);
            JsonObject data = new JsonObject();

            response.putObject("data", data);
            Long incr_val = result.getLong("result");

            data.putNumber("value", incr_val);

            if (incr_val != -1) {
                response.putBoolean("success", true);
            } else {
                response.putBoolean("success", false);
                response.putString("reason", "Key doesn't exist, or value is non-number '" + Util.getKey(message) + "'");
            }
            return response;
        }
    },
    DECR() {
        @Override
        public JsonObject operation(CouchbaseClient cb, Message<JsonObject> message) throws Exception {

            String key = Util.getKey(message);
            Number by = message.body().getNumber("by") == null? 1 : message.body().getNumber("by");

            JsonObject result = new JsonObject();
            if (Util.isInteger(by)) {
                result.putNumber("result", cb.decr(key, (int) by));
            } else {
                result.putNumber("result", cb.decr(key, (long)by));
            }
            return result;
        }

        @Override
        public JsonObject buildResponse(Message<JsonObject> message, JsonObject result, boolean returnAcknowledgement) throws Exception {

            if(!returnAcknowledgement) {
                return null;
            }

            JsonObject response = Util.createGenericResponse(message);
            JsonObject data = new JsonObject();

            response.putObject("data", data);
            Long decr_val = result.getLong("result");
            data.putNumber("value", decr_val);

            if (decr_val != -1) {
                response.putBoolean("success", true);
            } else {
                response.putBoolean("success", false);
                response.putString("reason", "Key doesn't exist, or value is non-number '" + Util.getKey(message) + "'");
            }

            return response;
        }
    },


    /* 
    * Storage Operations 
    * SET, ADD, REPLACE, CAS, APPEND, PREPEND, TOUCH
    * 
    */

    
    SET() {
        @Override
        public JsonObject operation(CouchbaseClient cb, Message<JsonObject> message) throws Exception {

            String key = Util.getKey(message);
            Object value = Util.getValue(message);
            int expires = message.body().getInteger("expiry") == null ? 0 : message.body().getInteger("expiry");
            PersistTo persistTo = (message.body().getInteger("persistTo") == null ? PersistTo.ZERO : PersistTo.values()[message.body().getInteger("persistTo")]);
            ReplicateTo replicateTo = (message.body().getInteger("replicateTo") == null ? ReplicateTo.ZERO : ReplicateTo.values()[message.body().getInteger("replicateTo")]);

            // Debug
//            System.out.println("value: " + value.toString() + " is of type " + value.getClass().toString());

            OperationFuture<Boolean> op = cb.set(key, expires, value, persistTo, replicateTo);

            JsonObject result = new JsonObject();
            result.putBoolean("success", op.get());
            result.putNumber("cas", op.getCas());
            return result;
        }

        @Override
        public JsonObject buildResponse(Message<JsonObject> message, JsonObject result, boolean returnAcknowledgement) throws Exception {
            if(!returnAcknowledgement) {
                return null;
            }

            JsonObject response = Util.createGenericResponse(message);

            if (result.getBoolean("success")) {
                response.putBoolean("success", true);
            } else {
                response.putBoolean("success", false);
            }

//            response.putObject("response", result);
            return response;
        }
    },
    ADD() {
        @Override
        public JsonObject operation(CouchbaseClient cb, Message<JsonObject> message) throws Exception {

            String key = Util.getKey(message);
            Object value = Util.getValue(message);
            Integer exp = message.body().getInteger("expiry") == null ? 0 : message.body().getInteger("expiry");
            PersistTo persistTo = (message.body().getInteger("persistTo") == null ? PersistTo.ONE : PersistTo.values()[message.body().getInteger("persistTo")]);
            ReplicateTo replicateTo = (message.body().getInteger("replicateTo") == null ? ReplicateTo.ZERO : ReplicateTo.values()[message.body().getInteger("replicateTo")]);

            OperationFuture<Boolean> op = cb.add(key, exp, value, persistTo, replicateTo);

            JsonObject result = new JsonObject();
            result.putBoolean("success", op.get());
            result.putNumber("cas", op.getCas());
            return result;
        }

        @Override
        public JsonObject buildResponse(Message<JsonObject> message, JsonObject result, boolean returnAcknowledgement) throws Exception {
            if(!returnAcknowledgement) {
                return null;
            }

            JsonObject response = Util.createGenericResponse(message);

            if (result.getBoolean("success")) {
                response.putBoolean("success", true);
            } else {
                response.putBoolean("success", false);
                result.putString("reason", "failed to fetch key '" + Util.getKey(message) + "'");
            }

            response.putObject("response", result);
            return response;
        }
    },
    REPLACE() {
        @Override
        public JsonObject operation(CouchbaseClient cb, Message<JsonObject> message) throws Exception {

            String key = Util.getKey(message);
            Object value = Util.getValue(message);
            Integer exp = message.body().getInteger("expiry") == null ? 0 : message.body().getInteger("expiry");
            Long cas = message.body().getLong("cas") == null ? null : message.body().getLong("cas");
            PersistTo persistTo = (message.body().getInteger("persistTo") == null ? null : PersistTo.values()[message.body().getInteger("persistTo")]);
            ReplicateTo replicateTo = (message.body().getInteger("replicateTo") == null ? null : ReplicateTo.values()[message.body().getInteger("replicateTo")]);

            OperationFuture<Boolean> op = cb.replace(key, exp, value, persistTo, replicateTo);

            JsonObject result = new JsonObject();
            result.putBoolean("success", op.get());
            result.putNumber("cas", op.getCas());
            return result;
        }

        @Override
        public JsonObject buildResponse(Message<JsonObject> message, JsonObject result, boolean returnAcknowledgement) throws Exception {
            if(!returnAcknowledgement) {
                return null;
            }

            JsonObject response = Util.createGenericResponse(message);

            if (result.getBoolean("success")) {
                response.putBoolean("success", true);
            } else {
                response.putBoolean("success", false);
            }

            response.putObject("response", result);
            return response;
        }
    },
    CAS() {
        @Override
        public JsonObject operation(CouchbaseClient cb, Message<JsonObject> message) throws Exception {

            String key = Util.getKey(message);
            Object value = Util.getValue(message);
            Long cas = message.body().getLong("cas") == null ? null : message.body().getLong("cas");

            if (cas == null || cas <= 0) {
                throw new Exception("Missing mandatory non-empty positive long int field 'cas'");
            }
            Integer exp = message.body().getInteger("expiry") == null ? null : message.body().getInteger("expiry");
            PersistTo persistTo = (message.body().getInteger("persistTo") == null ? null : PersistTo.values()[message.body().getInteger("persistTo")]);
            ReplicateTo replicateTo = (message.body().getInteger("replicateTo") == null ? null : ReplicateTo.values()[message.body().getInteger("replicateTo")]);

            // This might not be a safe thing to do as the value can change
            Boolean get_cas_after = message.body().getBoolean("get_cas_after") == null ? false : message.body().getBoolean("get_cas_after");

            CASResponse op = cb.cas(key, cas, value, persistTo, replicateTo);
            JsonObject result = new JsonObject();

            if (op.equals(CASResponse.OK)) {
                result.putBoolean("success", true);
                result.putBoolean("casmatch", true);
                result.putBoolean("observe_success", true);
            }
            else if (op.equals(CASResponse.NOT_FOUND)) {
                result.putBoolean("success", false);
                result.putBoolean("exists", false);
                result.putBoolean("casmatch", false);
                result.putString("error", "key doesn't exist (CASResponse.NOT_FOUND)");
            }
            else if (op.equals(CASResponse.EXISTS)) {
                result.putBoolean("success", false);
                result.putBoolean("exists", true);
                result.putBoolean("casmatch", false);
                result.putString("error", "cas mismatch (CASResponse.EXISTS)");
            }
            else if (op.equals(CASResponse.OBSERVE_ERROR_IN_ARGS)) {
                result.putBoolean("success", false);
                result.putBoolean("exists", false);
                result.putBoolean("casmatch", false);
                result.putString("error", "error in observe arguments (CASResponse.OBSERVE_ERROR_IN_ARGS)");
            }
            else if (op.equals(CASResponse.OBSERVE_MODIFIED)) {
                result.putBoolean("success", true);
                result.putBoolean("exists", true);
                result.putBoolean("casmatch", true);
                result.putBoolean("observe_success", false);
                result.putString("error", "value changed during observe (CASResponse.MODIFIED)");
            }

            if (exp != null && exp > 0 && op.equals(CASResponse.OK)) {
                OperationFuture<Boolean> touch = cb.touch(key, exp);
                result.putNumber("cas", touch.getCas());
            }
            else if (get_cas_after) {
                CASValue status = cb.gets(key);
                result.putNumber("cas", status.getCas());
                result.putString("get_cas_message", "Result was " + status.getValue().toString());
            }

            return result;

        }

        @Override
        public JsonObject buildResponse(Message<JsonObject> message, JsonObject result, boolean returnAcknowledgement) throws Exception {
            if(!returnAcknowledgement) {
                return null;
            }

            JsonObject response = Util.createGenericResponse(message);

            if (result.getBoolean("success")) {
                response.putBoolean("success", true);
            } else {
                response.putBoolean("success", false);
            }

            response.putObject("response", result);
            return response;
        }
    },
    APPEND() {
        @Override
        public JsonObject operation(CouchbaseClient cb, Message<JsonObject> message) throws Exception {

            String key = Util.getKey(message);
            Object value = Util.getValue(message);
            Long cas = message.body().getLong("cas");

            if (cas == null) {
                throw new Exception("Missing mandatory non-empty field 'cas'");
            }

            OperationFuture<Boolean> op = cb.append(cas, key, value);

            JsonObject result = new JsonObject();
            result.putBoolean("success", op.get());
            result.putNumber("cas", op.getCas());
            return result;
        }

        @Override
        public JsonObject buildResponse(Message<JsonObject> message, JsonObject result, boolean returnAcknowledgement) throws Exception {
            if(!returnAcknowledgement) {
                return null;
            }
            JsonObject response = Util.createGenericResponse(message);

            if (result.getBoolean("success")) {
                response.putBoolean("success", true);
            } else {
                response.putBoolean("success", false);
            }

            response.putObject("response", result);

            return response;
        }
    },
    PREPEND() {
        @Override
        public JsonObject operation(CouchbaseClient cb, Message<JsonObject> message) throws Exception {
            String key = Util.getKey(message);
            Object value = Util.getValue(message);
            Long cas = message.body().getLong("cas");

            if (cas == null) {
                throw new Exception("Missing mandatory non-empty field 'cas'");
            }

            OperationFuture<Boolean> op = cb.prepend(cas, key, value);

            JsonObject result = new JsonObject();
            result.putBoolean("success", op.get());
            result.putNumber("cas", op.getCas());
            return result;
        }

        @Override
        public JsonObject buildResponse(Message<JsonObject> message, JsonObject result, boolean returnAcknowledgement) throws Exception {
            if(!returnAcknowledgement) {
                return null;
            }

            JsonObject response = Util.createGenericResponse(message);

            if (result.getBoolean("success")) {
                response.putBoolean("success", true);
            } else {
                response.putBoolean("success", false);
            }

            response.putObject("response", result);
            return response;
        }
    },
    TOUCH() {
        @Override
        public JsonObject operation(CouchbaseClient cb, Message<JsonObject> message) throws Exception {

            String key = Util.getKey(message);
            Integer exp = message.body().getInteger("expiry");

            if (exp == null) {
                throw new Exception("missing mandatory non-empty field 'exp'");
            }
            OperationFuture<Boolean> op = cb.touch(key, exp.intValue());

            JsonObject result = new JsonObject();
            result.putBoolean("success", op.get());
            result.putNumber("cas", op.getCas());
            return result;
        }

        @Override
        public JsonObject buildResponse(Message<JsonObject> message, JsonObject result, boolean returnAcknowledgement) throws Exception {
            if(!returnAcknowledgement) {
                return null;
            }
            JsonObject response = Util.createGenericResponse(message);

            if (result.getBoolean("success")) {
                response.putBoolean("success", true);
            } else {
                response.putBoolean("success", false);
            }

            response.putObject("response", result);

            return response;
        }
    },


    /*
    * Retrieval Operations
    * GET, MULTIGET, GETANDTOUCH(GAT), STATUS
    *
    */


    GET() {
        @Override
        public JsonObject operation(CouchbaseClient cb, Message<JsonObject> message) throws Exception {
            String key = Util.getKey(message);
            Object o = cb.get(key);


            JsonObject result = new JsonObject();

            if (o != null) {
                JsonObject data = new JsonObject();
                data = Util.parseForJson(data, "value", o);

                result.putBoolean("success", true);
                result.putBoolean("exists", true);
                result.putObject("value", data);

            }
            else {
                result.putBoolean("success", true);
                result.putBoolean("exists", false);
                result.putString("value", "");
            }

            return result;
        }

        @Override
        public JsonObject buildResponse(Message<JsonObject> message, JsonObject result, boolean returnAcknowledgement) throws Exception {

            // Always return data when doing cb.get()
            // if(!returnAcknowledgement) {
            //     return null;
            // }

            JsonObject response = Util.createGenericResponse(message);
            JsonObject data = new JsonObject();

            response.putString("key", message.body().getString("key"));
//            data = Util.parseForJson(data, "value", result);

            if (result == null)
                response.putBoolean("exists", false);
            else
                response.putBoolean("exists", true);

            response.putObject("data", result.getObject("value"));
            response.putBoolean("success", true);
            return response;
        }
    },
    MULTIGET() {
        @Override
        public JsonObject operation(CouchbaseClient cb, Message<JsonObject> message) throws Exception {

            // Always return data when doing cb.get()
            // if(!returnAcknowledgement) {
            //     return null;
            // }

            JsonArray keys = message.body().getArray("keys");
            if (keys == null || keys.size() == 0) {
                throw new Exception("Missing mandatory non-empty field 'keys'");
            }
            List<String> keysList = new ArrayList<String>();
            for (Object o : keys.toArray()) {
                keysList.add((String) o);
            }


            Map<String, Object> bulk = cb.getBulk(keysList);

            JsonObject result = new JsonObject();
            JsonObject data = new JsonObject();

            for (String k : bulk.keySet()) {
                Object value = bulk.get(k);
                data = Util.parseForJson(data, k, value);
            }

            result.putObject("values", data);
            return result;
        }

        @Override
        public JsonObject buildResponse(Message<JsonObject> message, JsonObject result, boolean returnAcknowledgement) throws Exception {
            // Always return data when doing cb.get()
            // if(!returnAcknowledgement) {
            //     return null;
            // }

            JsonObject response = Util.createGenericResponse(message);
            JsonObject data = new JsonObject();

            if (result.getBoolean("success")) {
                response.putBoolean("success", true);
            } else {
                response.putBoolean("success", false);
            }

            response.putObject("response", result);

            return response;
        }
    },
    GAT() {
        @Override
        public JsonObject operation(CouchbaseClient cb, Message<JsonObject> message) throws Exception {
            String key = Util.getKey(message);
            Integer exp = message.body().getInteger("expiry");
            if (exp == null) {
                throw new Exception("Missing mandatory non-empty field 'expiry'");
            }
            CASValue<Object> op = cb.getAndTouch(key, exp);

            Object o = op.getValue();

            JsonObject result = new JsonObject();

            if (o != null) {
                JsonObject data = new JsonObject();
                data = Util.parseForJson(data, "value", o);

                result.putBoolean("success", true);
                result.putNumber("cas", op.getCas());
                result.putObject("value", data);
            }
            else {
                result.putBoolean("success", false);
                result.putString("value", "");
                result.putNumber("cas", 0);
                result.putString("error", "key doesn't exist, or failed to get");
            }

            return result;
        }

        @Override
        public JsonObject buildResponse(Message<JsonObject> message, JsonObject result, boolean returnAcknowledgement) throws Exception {

            // Always return data when doing cb.get()
            // if(!returnAcknowledgement) {
            //     return null;
            // }

            JsonObject response = Util.createGenericResponse(message);

            if (result.getBoolean("success")) {
                response.putBoolean("success", true);
            } else {
                response.putBoolean("success", false);
            }

            response.putObject("response", result);

            return response;
        }
    },

    /*
    * Deletion Operations
    * DELETE, FLUSH
    *
    */

    DELETE() {
        @Override
        public JsonObject operation(CouchbaseClient cb, Message<JsonObject> message) throws Exception {
            String key = Util.getKey(message);
            OperationFuture<Boolean> op = cb.delete(key);

            JsonObject result = new JsonObject();
            result.putBoolean("success", op.get());
            return result;
        }

        @Override
        public JsonObject buildResponse(Message<JsonObject> message, JsonObject result, boolean returnAcknowledgement) throws Exception {
            if(!returnAcknowledgement) {
                return null;
            }
            JsonObject response = Util.createGenericResponse(message);

            if (result.getBoolean("success")) {
                response.putBoolean("success", true);
            } else {
                response.putBoolean("success", false);
            }

            response.putObject("response", result);

            return response;
        }
    },
    FLUSH() {
        @Override
        public JsonObject operation(CouchbaseClient cb, Message<JsonObject> message) throws Exception {
            int delay = message.body().getInteger("delay") == null ? 0 : message.body().getInteger("delay");
            OperationFuture<Boolean> op = cb.flush(delay);

            JsonObject result = new JsonObject();
            result.putBoolean("success", op.get());
            return result;
        }

        @Override
        public JsonObject buildResponse(Message<JsonObject> message, JsonObject result, boolean returnAcknowledgement) throws Exception {
            if(!returnAcknowledgement) {
                return null;
            }
            JsonObject response = Util.createGenericResponse(message);

            if (result.getBoolean("success")) {
                response.putBoolean("success", true);
            } else {
                response.putBoolean("success", false);
            }

            response.putObject("response", result);

            return response;
        }
    };

    //no default implementation
    public abstract JsonObject operation(CouchbaseClient cb, Message<JsonObject> message) throws Exception;

    public abstract JsonObject buildResponse(Message<JsonObject> message, JsonObject result, boolean returnAcknowledgement) throws Exception;
}
