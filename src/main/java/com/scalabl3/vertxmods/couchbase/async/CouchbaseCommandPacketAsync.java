package com.scalabl3.vertxmods.couchbase.async;
import com.couchbase.client.CouchbaseClient;
import com.couchbase.client.internal.HttpFuture;
import com.couchbase.client.protocol.views.DesignDocument;
import com.couchbase.client.protocol.views.Query;
import com.couchbase.client.protocol.views.View;
import com.couchbase.client.protocol.views.ViewResponse;
import net.spy.memcached.CASResponse;
import net.spy.memcached.CASValue;
import net.spy.memcached.PersistTo;
import net.spy.memcached.ReplicateTo;
import net.spy.memcached.internal.BulkFuture;
import net.spy.memcached.internal.GetFuture;
import net.spy.memcached.internal.OperationFuture;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;


@SuppressWarnings("unchecked")
public enum CouchbaseCommandPacketAsync {


    /*
    Delete Design Doc
     */

    DELETEDESIGNDOC() {
        @Override
        public Future operation(CouchbaseClient cb, Message<JsonObject> message) throws Exception {
            String name = message.body().getString("name");
            HttpFuture<Boolean> f = cb.asyncDeleteDesignDoc(name);
            return f;
        }

        @Override
        public JsonObject buildResponse(Message<JsonObject> message, Future future, boolean returnAcknowledgement) throws Exception {

            if(!returnAcknowledgement) {
                return null;
            }

            checkTimeout(future);
            JsonObject response = createGenericResponse(message);
            response.putBoolean("success", (Boolean)future.get());
            return response;
        }
    },

    /*
    Create Design Doc
     */
    CREATEDESIGNDOC() {
        @Override
        public Future operation(CouchbaseClient cb, Message<JsonObject> message) throws Exception {
            String name = message.body().getString("name");
            String value = message.body().getString("value");
            HttpFuture<Boolean> f = cb.asyncCreateDesignDoc(name, value);
            return f;
        }

        @Override
        public JsonObject buildResponse(Message<JsonObject> message, Future future, boolean returnAcknowledgement) throws Exception {

            if(!returnAcknowledgement) {
                return null;
            }

            checkTimeout(future);
            JsonObject response = createGenericResponse(message);
            response.putBoolean("success", (Boolean)future.get());
            return response;
        }
    },

    /*
    Get Design Doc
     */
    GETDESIGNDOC() {
        @Override
        public Future operation(CouchbaseClient cb, Message<JsonObject> message) throws Exception {
            String design_doc = message.body().getString("name");
//            System.out.println("GETDESIGNDOC: getting " + design_doc);
            HttpFuture<DesignDocument> f = cb.asyncGetDesignDoc(design_doc);
            return f;
        }

        @Override
        public JsonObject buildResponse(Message<JsonObject> message, Future future, boolean returnAcknowledgement) throws Exception {

            if(!returnAcknowledgement) {
                return null;
            }

            checkTimeout(future);
            JsonObject response = createGenericResponse(message);

            DesignDocument value = (DesignDocument) future.get();

            // if we made it this far its a success!
            response.putBoolean("exists", true);
            response.putObject("data", new JsonObject(value.toJson()));
            response.putBoolean("success", true);

            return response;
        }
    },



    /*
       Query Views
     */
    QUERY() {
        @Override
        public Future operation(CouchbaseClient cb, Message<JsonObject> message) throws Exception {

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
                query.setKey(key);
            }

            // was keys sent
            if (keys != null) {
                query.setKeys(String.valueOf(keys));
            }

            query.setIncludeDocs(include_docs);

            HttpFuture<ViewResponse> response = cb.asyncQuery(view, query);
            return response;

        }

        @Override
        public JsonObject buildResponse(Message<JsonObject> message, Future future, boolean returnAcknowledgement) throws Exception {

            if(!returnAcknowledgement) {
                return null;
            }

            checkTimeout(future);

            JsonObject response = createGenericResponse(message);
            ViewResponse futureResponse = (ViewResponse)future.get();
            response.putBoolean("success", true);


            JsonObject result = new JsonObject();

//            System.out.println("Future is: " + futureResponse.toString());

            result.putArray("result", new JsonArray(futureResponse.getMap().values().toArray()));

//            for (ViewRow row : futureResponse) {
//                result.putArray("result", new JsonArray(row.getValue()));
//            }

            response.putObject("response", result);
            return response;
        }
    },


    /*
    * Atomic Counter Operations
    * INCR, DECR
    *
    */


    INCR() {
        @Override
        public Future operation(CouchbaseClient cb, Message<JsonObject> message) throws Exception {
            String key = getKey(message);

            Number by = message.body().getNumber("by") == null? 1 : message.body().getNumber("by");
            OperationFuture<Long> operationFuture;

            if (by == (int)by) {
                operationFuture = cb.asyncIncr(key, (int)by);
            } else {
                operationFuture = cb.asyncIncr(key, (long)by);
            }

            return operationFuture;

//            if (create) {
//                cb.incr(key, delta, default_value);
//                return null;
//            }
//            else {
//
//                return operationFuture;
//            }
        }

        @Override
        public JsonObject buildResponse(Message<JsonObject> message, Future future, boolean returnAcknowledgement) throws Exception {

            if(!returnAcknowledgement) {
                return null;
            }
            checkTimeout(future);
            JsonObject response = createGenericResponse(message);
            JsonObject data = new JsonObject();

            response.putObject("data", data);

//            response.putString("future.get()", future.get().toString());

            Long incr_val = (Long) future.get();

            if (incr_val != null) {
                response.putBoolean("success", true);
                data.putNumber("value", incr_val);
            } else {
                response.putBoolean("success", false);
                response.putString("reason", "failed to fetch key '" + getKey(message) + "'");
            }
            return response;
        }
    },
    DECR() {
        @Override
        public Future operation(CouchbaseClient cb, Message<JsonObject> message) throws Exception {
            String key = getKey(message);
            Long by = message.body().getLong("by");
            if (by == null) {
                throw new Exception("missing mandatory non-empty field 'by'");
            }
            OperationFuture<Long> operationFuture = cb.asyncDecr(key, by);
            return operationFuture;
        }

        @Override
        public JsonObject buildResponse(Message<JsonObject> message, Future future, boolean returnAcknowledgement) throws Exception {
            if(!returnAcknowledgement) {
                return null;
            }
            checkTimeout(future);
            JsonObject response = createGenericResponse(message);
            JsonObject data = new JsonObject();
            response.putObject("data", data);
            Long decr_val = (Long) future.get();
            if (decr_val != null) {
                response.putBoolean("success", true);
                data.putNumber("value", decr_val);
            } else {
                response.putBoolean("success", false);
                response.putString("reason", "failed to fetch key '" + getKey(message) + "'");
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
        public Future operation(CouchbaseClient cb, Message<JsonObject> message) throws Exception {
            String key = getKey(message);

            Object value = message.body().getField("value");
            int expires = message.body().getInteger("expiry") == null ? 0 : message.body().getInteger("expiry");
            PersistTo persistTo = (message.body().getInteger("persistTo") == null ? PersistTo.ZERO : PersistTo.values()[message.body().getInteger("persistTo")]);
            ReplicateTo replicateTo = (message.body().getInteger("replicateTo") == null ? ReplicateTo.ZERO : ReplicateTo.values()[message.body().getInteger("replicateTo")]);

//            System.out.println("persistTo: " + persistTo);

            OperationFuture<Boolean> future = cb.set(key, expires, value, persistTo, replicateTo);

            return future;
        }

        @Override
        public JsonObject buildResponse(Message<JsonObject> message, Future future, boolean returnAcknowledgement) throws Exception {
            if(!returnAcknowledgement) {
                return null;
            }
            checkTimeout(future);
            JsonObject response = createGenericResponse(message);
            response.putBoolean("success", future != null);
            if (future == null) {
                response.putString("reason", "operation timed out");
            }
            return response;
        }
    },
    ADD() {
        @Override
        public Future operation(CouchbaseClient cb, Message<JsonObject> message) throws Exception {
            String key = getKey(message);

            Integer exp = message.body().getInteger("expiry") == null ? 0 : message.body().getInteger("expiry");
            PersistTo persistTo = (message.body().getInteger("persistTo") == null ? PersistTo.ZERO : PersistTo.values()[message.body().getInteger("persistTo")]);
            ReplicateTo replicateTo = (message.body().getInteger("replicateTo") == null ? ReplicateTo.ZERO : ReplicateTo.values()[message.body().getInteger("replicateTo")]);

            Object value = message.body().getField("value");
            OperationFuture<Boolean> operationFuture = cb.add(key, exp, value);
            return operationFuture;
        }

        @Override
        public JsonObject buildResponse(Message<JsonObject> message, Future future, boolean returnAcknowledgement) throws Exception {
            if(!returnAcknowledgement) {
                return null;
            }
            checkTimeout(future);
            JsonObject response = createGenericResponse(message);
//            JsonObject data = new JsonObject();
//            response.putObject("data", data);
            boolean success = (Boolean)future.get();
            response.putBoolean("success", success);
            if (!success) {
                response.putString("reason", "failed to fetch key '" + getKey(message) + "'");
            }

            return response;
        }
    },
    REPLACE() {
        @Override
        public Future operation(CouchbaseClient cb, Message<JsonObject> message) throws Exception {
            String key = getKey(message);

            Integer exp = message.body().getInteger("expiry") == null ? 0 : message.body().getInteger("expiry");
            Integer persistTo = message.body().getInteger("persistTo") == null ? -1 : message.body().getInteger("persistTo");
            Integer replicateTo = message.body().getInteger("replicateTo") == null ? -1 : message.body().getInteger("replicateTo");
            Long cas = message.body().getLong("cas") == null ? -1 : message.body().getLong("cas");

            Object value = message.body().getField("value");
            OperationFuture<Boolean> operationFuture = cb.replace(key, exp, value);
            return operationFuture;
        }

        @Override
        public JsonObject buildResponse(Message<JsonObject> message, Future future, boolean returnAcknowledgement) throws Exception {
            checkTimeout(future);
            JsonObject response = createGenericResponse(message);
            JsonObject data = new JsonObject();
            response.putObject("data", data);
            boolean success = (Boolean) future.get();
            response.putBoolean("success", success);
            if (!success) {
                response.putString("reason", "failed to fetch key '" + getKey(message) + "'");
            }
            return response;
        }
    },
    CAS() {
        @Override
        public Future operation(CouchbaseClient cb, Message<JsonObject> message) throws Exception {

            String key = getKey(message);

            //Integer exp = message.body().getInteger("expiry") == null ? 0 : message.body().getInteger("expiry");
            Long cas = message.body().getLong("cas") == null ? -1 : message.body().getLong("cas");

            Object value = message.body().getField("value");



            OperationFuture<CASResponse> operationFuture = cb.asyncCAS(key, cas, value);

            //if (persistTo < 0 && replicateTo < 0)
            //    operationFuture = cb.cas(key, cas, value, persistTo);
            //else if (persistTo > 0)
            //    operationFuture = cb.cas(key, cas, value, persistTo);
            //else

            return operationFuture;
        }

        @Override
        public JsonObject buildResponse(Message<JsonObject> message, Future future, boolean returnAcknowledgement) throws Exception {
            checkTimeout(future);
            JsonObject response = createGenericResponse(message);
            JsonObject data = new JsonObject();
            response.putObject("data", data);
            boolean success = (Boolean) future.get();
            response.putBoolean("success", success);
            if (!success) {
                response.putString("reason", "failed to fetch key '" + getKey(message) + "'");
            }
            return response;
        }
    },
    APPEND() {
        @Override
        public Future operation(CouchbaseClient cb, Message<JsonObject> message) throws Exception {
            String key = getKey(message);
            Long cas = message.body().getLong("cas");
            if (cas == null) {
                throw new Exception("missing mandatory non-empty field 'cas'");
            }
            Object value = message.body().getField("value");
            OperationFuture<Boolean> operationFuture = cb.append(cas, key, value);
            return operationFuture;
        }

        @Override
        public JsonObject buildResponse(Message<JsonObject> message, Future future, boolean returnAcknowledgement) throws Exception {
            if(!returnAcknowledgement) {
                return null;
            }
            checkTimeout(future);
            JsonObject response = createGenericResponse(message);
            JsonObject data = new JsonObject();
            response.putObject("data", data);
            boolean success = (Boolean)future.get();
            response.putBoolean("success", success);
            if (!success) {
                response.putString("reason", "failed to fetch key '" + getKey(message) + "'");
            }

            return response;
        }
    },
    PREPEND() {
        @Override
        public Future operation(CouchbaseClient cb, Message<JsonObject> message) throws Exception {
            String key = getKey(message);
            Long cas = message.body().getLong("cas");
            if (cas == null) {
                throw new Exception("missing mandatory non-empty field 'cas'");
            }
            Object value = message.body().getField("value");
            OperationFuture<Boolean> operationFuture = cb.prepend(cas, key, value);
            return operationFuture;
        }

        @Override
        public JsonObject buildResponse(Message<JsonObject> message, Future future, boolean returnAcknowledgement) throws Exception {
            if(!returnAcknowledgement) {
                return null;
            }
            checkTimeout(future);
            JsonObject response = createGenericResponse(message);
            JsonObject data = new JsonObject();
            response.putObject("data", data);
            boolean success = (Boolean)future.get();
            response.putBoolean("success", success);
            if (!success) {
                response.putString("reason", "failed to fetch key '" + getKey(message) + "'");
            }

            return response;
        }
    },
    TOUCH() {
        @Override
        public Future operation(CouchbaseClient cb, Message<JsonObject> message) throws Exception {
            String key = getKey(message);
            Integer exp = message.body().getInteger("expiry");
            if (exp == null) {
                throw new Exception("missing mandatory non-empty field 'exp'");
            }
            OperationFuture<Boolean> operationFuture = cb.touch(key, exp.intValue());
            return operationFuture;
        }

        @Override
        public JsonObject buildResponse(Message<JsonObject> message, Future future, boolean returnAcknowledgement) throws Exception {
            if(!returnAcknowledgement) {
                return null;
            }
            checkTimeout(future);
            JsonObject response = createGenericResponse(message);
            JsonObject data = new JsonObject();
            response.putObject("data", data);
            boolean success = (Boolean) future.get();
            response.putBoolean("success", success);
            if (!success) {
                response.putString("reason", "failed to fetch key '" + getKey(message) + "'");
            }
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
        public Future operation(CouchbaseClient cb, Message<JsonObject> message) throws Exception {
            String key = getKey(message);
            GetFuture<Object> f = cb.asyncGet(key);
            return f;
        }

        @Override
        public JsonObject buildResponse(Message<JsonObject> message, Future future, boolean returnAcknowledgement) throws Exception {

            checkTimeout(future);
            JsonObject response = createGenericResponse(message);
            JsonObject data = new JsonObject();

            Object value = future.get();
            response.putString("key", message.body().getString("key"));


            data = parseForJson(data, "value", value);

            if (value == null)
                response.putBoolean("exists", false);
            else
                response.putBoolean("exists", true);


            response.putObject("data", data);
            response.putBoolean("success", true);

            return response;
        }
    },
    MULTIGET() {
        @Override
        public Future operation(CouchbaseClient cb, Message<JsonObject> message) throws Exception {
            JsonArray keys = message.body().getArray("keys");
            if (keys == null || keys.size() == 0) {
                throw new Exception("missing mandatory non-empty field 'keys'");
            }
            List<String> keysList = new ArrayList<>();
            for (Object o : keys.toArray()) {
                keysList.add((String) o);
            }
            BulkFuture<Map<String, Object>> bulkFuture = cb.asyncGetBulk(keysList);
            return bulkFuture;
        }

        @Override
        public JsonObject buildResponse(Message<JsonObject> message, Future future, boolean returnAcknowledgement) throws Exception {
            checkTimeout(future);
            JsonObject response = createGenericResponse(message);
            JsonObject data = new JsonObject();
            response.putObject("data", data);
            Map<String, Object> result = (Map<String, Object>) future.get();
            for (String k : result.keySet()) {
                Object value = result.get(k);
                data = parseForJson(data, k, value);
            }
            response.putBoolean("success", true);

            return response;
        }
    },
    STATUS() {
        @Override
        public Future operation(CouchbaseClient cb, Message<JsonObject> message) throws Exception {
            Future<JsonArray[]> f = syncExecutor.submit(new StatusCallable(cb));
            return f;
        }

        @Override
        public JsonObject buildResponse(Message<JsonObject> message, Future future, boolean returnAcknowledgement) throws Exception {
            checkTimeout(future);
            JsonArray[] status = (JsonArray[]) future.get();
            JsonObject response = createGenericResponse(message);
            JsonObject data = new JsonObject();
            response.putObject("data", data);
            response.putBoolean("success", true);
            data.putArray("available", status[0]);
            data.putArray("unavailable", status[1]);
            return response;
        }
    },
    GAT() {
        @Override
        public Future operation(CouchbaseClient cb, Message<JsonObject> message) throws Exception {
            String key = getKey(message);
            Integer exp = message.body().getInteger("expiry");
            if (exp == null) {
                throw new Exception("missing mandatory non-empty field 'exp'");
            }
            OperationFuture<CASValue<Object>> operationFuture = cb.asyncGetAndTouch(key, exp);
            return operationFuture;
        }

        @Override
        public JsonObject buildResponse(Message<JsonObject> message, Future future, boolean returnAcknowledgement) throws Exception {
            checkTimeout(future);
            JsonObject response = createGenericResponse(message);
            JsonObject data = new JsonObject();
            response.putObject("data", data);
            CASValue<Object> value = (CASValue<Object>) future.get();
            if (value != null) {
                data = parseForJson(data, "key", value.getValue());
                Long c = value.getCas();
                if (c != null) {
                    data.putNumber("cas", value.getCas());
                }
                response.putBoolean("success", true);
            } else {
                response.putBoolean("success", false);
                response.putString("reason", "failed to fetch key '" + getKey(message) + "'");
            }

            return response;
        }
    },

    GETSTATS() {
        @Override
        public Future operation(CouchbaseClient cb, Message<JsonObject> message) throws Exception {
            Future<Map<SocketAddress, Map<String, String>>> f = syncExecutor.submit(new ClusterStatsCallable(cb));
            return f;
        }

        @Override
        public JsonObject buildResponse(Message<JsonObject> message, Future future, boolean returnAcknowledgement) throws Exception {
            checkTimeout(future);
            Map<SocketAddress, Map<String, String>> stats = (Map<SocketAddress, Map<String, String>>) future.get();
            JsonObject response = createGenericResponse(message);
            JsonObject data = new JsonObject();
            for (SocketAddress sa : stats.keySet()) {
                JsonObject s = new JsonObject();
                data.putObject("server", s);
                s.putString("address", ((InetSocketAddress) sa).getHostString() + ":" + ((InetSocketAddress) sa).getPort());
                Map<String, String> info = stats.get(sa);
                for (String i : info.keySet()) {
                    s.putString(i, info.get(i));
                }
            }
            response.putObject("data", data);
            response.putBoolean("success", true);
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
        public Future operation(CouchbaseClient cb, Message<JsonObject> message) throws Exception {
            String key = getKey(message);
            OperationFuture<Boolean> operationFuture = cb.delete(key);
            return operationFuture;
        }

        @Override
        public JsonObject buildResponse(Message<JsonObject> message, Future future, boolean returnAcknowledgement) throws Exception {
            if(!returnAcknowledgement) {
                return null;
            }
            checkTimeout(future);
            JsonObject response = createGenericResponse(message);
            JsonObject data = new JsonObject();
            response.putObject("data", data);
            boolean success = (Boolean) future.get();
            response.putBoolean("success", success);
            if (!success) {
                response.putString("reason", "failed to fetch key '" + getKey(message) + "'");
            }
            return response;
        }
    },
    FLUSH() {
        @Override
        public Future operation(CouchbaseClient cb, Message<JsonObject> message) throws Exception {
            int delay = message.body().getInteger("delay") == null ? 0 : message.body().getInteger("delay");
            OperationFuture<Boolean> operationFuture = cb.flush(delay);
            return operationFuture;
        }

        @Override
        public JsonObject buildResponse(Message<JsonObject> message, Future future, boolean returnAcknowledgement) throws Exception {
            if(!returnAcknowledgement) {
                return null;
            }
            checkTimeout(future);
            JsonObject response = createGenericResponse(message);
            JsonObject data = new JsonObject();
            response.putObject("data", data);
            boolean success = (Boolean)future.get();
            response.putBoolean("success", success);
            return response;
        }
    };

    private static JsonObject createGenericResponse(Message<JsonObject> message) {
        JsonObject response = new JsonObject();
        response.putString("op", message.body().getString("op").toUpperCase());
        response.putString("key", message.body().getString("key"));
        response.putNumber("timestamp", System.currentTimeMillis());
        return response;
    }

    public static String voidNull(String s) {
        return s == null ? "" : s;
    }
    
    private static void checkTimeout(Future f) throws TimeoutException {
        if(f == null) {
            throw new TimeoutException();
        }
    }
    
    private static String getKey(Message<JsonObject> message) throws Exception {
        String key = voidNull(message.body().getString("key"));
        if (key.isEmpty()) {
            throw new Exception("missing mandatory non-empty field 'key'");
        }
        return key;
    }

    private static JsonObject parseForJson(JsonObject jsonObject, String key, Object value) throws Exception {
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
                throw new Exception("unsupported object type");
            }
        }
        return jsonObject;
    }

    private static ExecutorService syncExecutor = Executors.newFixedThreadPool(2);

    //no default implementation
    public abstract Future operation(CouchbaseClient cb, Message<JsonObject> message) throws Exception;

    public abstract JsonObject buildResponse(Message<JsonObject> message, Future future, boolean returnAcknowledgement) throws Exception;
}
