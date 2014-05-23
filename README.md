# VertX mod CouchBase

a couchbase eventbus worker. 
This worker connects to your couchbase cluster and listens on the VertX eventbus for queries.

This is a gradle-fied and modified version of scalabl3/vertx-couchbase, Things I am adding:

VIEW based queries
Boot class ( read config, boot async/sync mode )
Testing ( sync )



## Building
see "gradlew tasks"

## Deploying to you Repo
Depending on your ~/.m2/settings.xml
see "gradlew install"

## Configure
### basic
couchbase mod accepts a json conf file as per vertx standards. 

```json
// Example couchbase mod configuration

{
  "async_mode": false, // tells the Boot class to start async or sync mode
  "address": "vertx.couchbase.sync", // the eventbus address this module listens on
  "couchbase.nodelist": "localhost:8091", // comma separated list of couchbase nodes
  "couchbase.bucket": "ivault", // the bucket to connect to
  "couchbase.bucket.password": "", // password if any for the bucket
  "couchbase.num.clients": 4 // number of clients to open towards the couch instances
}
```

### Clustering
if running fatjars, you can specify you own cluster.xml like:
"java -jar some-fatjar.jar -cp /path/to/conf/dir -cluster -conf /path/to/conf/dir/config.json"

if running via vertx itself you need to edit VERTX_HOME/conf/cluster.xml

See hazelcast documentation and vertx documentation for more info.


## Running
### runmod
vertx runmod com.scalabl3~vertxmods.couchbase~1.0.0-final -conf conf.json -cluster

### fatJar
if you built a fat-jar with gradlew fatJar, you can run it like so:
```
CONF_DIR = dir where cluster.xml, langs.properties, conf.json lare
java -jar some-fatjar.jar -cp $CONF_DIR -cluster -conf $CONF_DIR/config.json
```

## Usage

A note about Async vs Sync, Sync should be used if you need confirmation only after persistTo or replicateTo have 
completed their quotas. Async mode does NOT support these yet until couch completes Observe. 

see: http://www.couchbase.com/wiki/display/couchbase/Observe

### General

act method for building request json object

```java

    public void act(HashMap<String, Object> cmd) {
        if (cmd == null)
            return;
    
        JsonObject notif = new JsonObject();
    
        for (String key : cmd.keySet()) {
            Object value = cmd.get(key);
    
            if (value != null) {
                if (value instanceof byte[])
                    notif.putBinary(key, (byte[]) value);
                else if (value instanceof Boolean)
                    notif.putBoolean(key, (Boolean) value);
                else if (value instanceof Number)
                    notif.putNumber(key, (Number) value);
                else if (value instanceof String)
                    notif.putString(key, (String) value);
                else if (value instanceof JsonArray)
                    notif.putArray(key, (JsonArray) value);
            }
        }
        System.out.println("sent: \n" + notif.encode());
        push(notif);
    }

```

push method for sending the request

```java

    private void push(JsonObject notif, Boolean async) {

        if (async) {

            Handler<Message<JsonObject>> async_replyHandler = new Handler<Message<JsonObject>>() {
                public void handle(Message<JsonObject> message) {
                    System.out.println("async received: \n" + message.body().encode());
                    post(message);
                }
            };
            vertx.eventBus().send(async_config.getString("address"), notif, async_replyHandler);
        
        } else {

            Handler<Message<JsonObject>> sync_replyHandler = new Handler<Message<JsonObject>>() {
                public void handle(Message<JsonObject> message) {
                    System.out.println("sync received: \n" + message.body().encode());
                    post(message);
                }
            };
            vertx.eventBus().send(sync_config.getString("address"), notif, sync_replyHandler);
        
        }
    }

```


### SET

```java

    ArrayList<String> x = new ArrayList<String>();
    x.add("couchbase");
    x.add("nuodb");
    
    cbop.put("op", "set");
    cbop.put("key", "op_get");
    cbop.put("value", encode(x));
    cbop.put("ack", true);
    act(cbop);

```


  {"response":{"op":"SET","key":"op_get","timestamp":1400848439299,"success":true,"response":{"success":true,"cas":18924200704321}}}
    Result: {"response":{"op":"SET","key":"op_get","timestamp":1400848439299,"success":true,"response":{"success":true,"cas":18924200704321}}}
    async received: 
    {"response":{"op":"SET","key":"op_get","timestamp":1400848439348,"success":true}}
    Result: {"response":{"op":"SET","key":"op_get","timestamp":1400848439348,"success":true}}



### Query

I have the following documents:

```json
{
   "username": "user10003",
   "password": "somepassword"
}

```
and the following view

```js
function (doc, meta) {
  emit([doc.username, doc.password], [doc.username]);
}
```

example query to send over eventbus in JSON.

```json
{
    "op":"QUERY",
    "design_doc":"users",
    "view_name":"users",
    "key":"[\"user0\",\"somepassword\"]",
    "include_docs":true,
    "ack":true
}
```

example of query construction in JAVA
```java
JsonObject request = new JsonObject().putString("op", "QUERY")
        .putString("design_doc", "users")
        .putString("view_name", "users")
        .putString("key", "user99990")
        .putBoolean("include_docs", true)
        .putBoolean("ack", true);
vertx.eventBus().send(config.getString("address"), request, new Handler<Message<JsonObject>>()...
```

### Performance Testing
A couple of tests for sync. 

./gradlew test -Dtest.single=QueryTests
./gradlew test -Dtest.single=Main

### SET

When creating documents, be sure to envode the valye portion with Gson

Example
```java

private String encode(Object val) {
        Gson gson = new Gson();
        return gson.toJson(val);
}

// simple userclass instance
User user = new User("someusername", "somepassword");

// put the object into the value portion of a op:ADD event
JsonObject request = new JsonObject().putString("op", "ADD")
                .putString("key", id.toString())
                .putString("value", encode(user)
                .putNumber("expiry", 300)
                .putBoolean("ack", true);
                
// send it off
vertx.eventBus().send("vertx.couchbase.sync", request, new Handler<Message<JsonObject>>()...

```

### GET

### QUERY