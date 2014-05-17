# VertX mod CouchBase

a couchbase eventbus worker.

This worker connects to your couchbase cluster and listens on the VertX eventbus for queries.

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
Data is requests is posted to this module via vertx's eventbus.

Simple Java Example
```java

private String encode(Object val) {
        Gson gson = new Gson();
        return gson.toJson(val);
}

JsonObject request = new JsonObject().putString("op", "ADD")
                .putString("key", id.toString())
                .putString("value", encode(new JsonObject()
                        .putString("username", "bob")
                        .putString("password", "cantguessthis!"))
                )
                .putNumber("expiry", 300)
                .putBoolean("ack", true);
                
vertx.eventBus().send("vertx.couchbase.sync", request, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                try {
                    System.out.print("done");
                } catch (Exception e) {
                    System.out.print("Error");
                }
            }
        });

```