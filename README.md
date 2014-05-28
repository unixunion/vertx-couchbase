# VertX mod CouchBase

a couchbase eventbus worker. 
This worker connects to your couchbase cluster and listens on the VertX eventbus for queries.

This is fork of scalabl3/vertx-couchbase, extended to support:

* View based queries
* Cluster management commands
* Boot class
* Testing testing testing
* Performance Testing

## Sync vs Async

Sync and Async differ slightly in that Sync mode accepts persistTo and replicateTo flags, which are blocking calls, So those flags are NOT supported in Async until couchbase supports Observe. See: [http://www.couchbase.com/wiki/display/couchbase/Observe]

All management commands currently only supported in Async mode.

## Building
This is a gradle project, so please see "gradlew tasks", typical options are:

### FatJar
```./gradlew fatJar```

Everything rolled into one huge jar which you can run with: "java -jar somefatjar.jar -cp somepath to resources -conf someconf.json -cluster"

See resources directory for default configs.

### Build
```./gradlew build```

Build the VertX module as per usual, the resulting module is normally run with "vertx run fully-qualified-module-name -conf someconf.json"

### ModZip
```./gradlew modZip```

Wrap the build up into zip which is portable without m2 repos. Run with "vertx runzip …"

### Install
```./gradlew install```

Send the module to your maven-like repo, depending on your ~/.m2/settings.xml
see "gradlew install"

## Configuration

couchbase mod accepts a json conf file as per vertx standards. this can be passed with -conf somefile.json on the command line. 

```json

// Example couchbase mod configuration

{
  "async_mode": true, // tells the Boot class to start async or sync mode
  "address": "vertx.couchbase.async", // the eventbus address this module listens on
  "couchbase.nodelist": "localhost:8091", // comma separated list of couchbase nodes
  "couchbase.bucket": "default", // the bucket to connect to
  "couchbase.bucket.password": "", // password if any for the bucket
  "couchbase.num.clients": 1, // number of clients to open towards the couch cluster
  "couchbase.manager": true, // start the manager command listener
  "couchbase.manager.username": "Administrator", // credentials to manager
  "couchbase.manager.password": "password", // credentials for manager
}

```

## Clustering
if running fatjars, you can specify you own cluster.xml like:
"java -jar some-fatjar.jar -cp /path/to/conf/dir -cluster -conf /path/to/conf/dir/config.json"

if running via vertx itself you can edit VERTX_HOME/conf/cluster.xml

See hazelcast documentation and vertx documentation for more info.


## Running
### runmod
```vertx runmod com.scalabl3~vertxmods.couchbase~1.0.0-final -conf conf.json -cluster```

### fatJar
if you built a fat-jar with gradlew fatJar, you can run it like so:
```
CONF_DIR = dir where cluster.xml, langs.properties, conf.json lare
java -jar some-fatjar.jar -cp $CONF_DIR -cluster -conf $CONF_DIR/config.json
```

## Usage

This module speaks JSON, so you will want to send messages as JSON to the module, and encode data/value portions of your documents with Gson, see the tests directory for examples.

### Commands

#### CREATEBUCKET

Creates a bucket 

##### request
```json
{
  "management": "CREATEBUCKET",
  "name": "test",
  "bucketType": "couchbase", // couchbase or memcached
  "memorySizeMB": 128,
  "replicas": 0, // number of nodes to replicate to 
  "authPassword": "", // desired password for the bucket
  "flushEnabled": true, // allow flushing
  "ack": false
}
```

##### response

```json
{
  "response": {
    "success": true
  }
}
```

#### DELETEBUCKET

Deletes a bucket

##### request

```json
{
  "management": "DELETEBUCKET",
  "name": "test",
  "ack": true
}
```

##### response

```json
{
  "response": {
    "success": true
  }
}

```

#### FLUSHBUCKET

Deletes all documents in a bucket

##### request

```json
{
  "management": "FLUSHBUCKET",
  "name": "test",
  "ack": true
}
```

##### response

```json
{
  "response": {
    "success": true
  }
}

```

#### LISTBUCKETS

##### request
```json
{
    "management":"LISTBUCKETS",
    "ack":true
}
```

##### response
```json
{
  "response": {
    "data": [
      "async",
      "default",
      "sync"
    ],
    "success": true
  }
}
```

#### CREATEDESIGNDOC

Create a design doc and views

##### request
```json

{
  "op": "CREATEDESIGNDOC",
  "name": "dev_test1",
  "value": "{\"language\":\"javascript\",\"views\":{\"view1\":{\"map\":\"function(a, b) {}\"}}}",
  "ack": true
}

```

##### response

```json

{
  "response": {
    "op": "CREATEDESIGNDOC",
    "key": null,
    "timestamp": 1400964336627,
    "success": true
  }
}

```

##### java example of request
For ease, I recommend using 
* com.couchbase.client.protocol.views.DesignDoc 
* com.couchbase.client.protocol.views.ViewDesign

Example

```java

        ViewDesign view1 = new ViewDesign(
                "view1",
                "function(a, b) {}"
        );

        DesignDocument dd = new DesignDocument("testtest");
        dd.setView(view1);

        JsonObject request = new JsonObject().putString("op", "CREATEDESIGNDOC")
                .putString("name", "dev_test1")
                .putString("value", dd.toJson())
                .putBoolean("ack", true);
                
```

#### GETDESIGNDOC

Returns the design doc for a view.

##### request
```json

{
    "op":"GETDESIGNDOC",
    "design_doc":"dev_test",
    "ack":true
}

```

##### response

```json

{
  "response": {
    "op": "GETDESIGNDOC",
    "key": null,
    "timestamp": 1400959460270,
    "exists": true,
    "data": {
      "language": "javascript",
      "views": {
        "test": {
          "map": "function (doc, meta) {\n  emit(meta.id, null);\n}"
        }
      }
    },
    "success": true
  }
}

```

#### DELETEDESIGNDOC

##### request
```json

{
  "op": "DELETEDESIGNDOC",
  "name": "dev_test",
  "ack": true
}

```

##### response

```json

{
  "response": {
    "op": "DELETEDESIGNDOC",
    "key": null,
    "timestamp": 1400970226990,
    "success": true
  }
}

```

#### SET

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



#### Query

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

### Performance and Testing
A couple of tests for sync. 

#### General Tests
./gradlew test -Dtest.single=QueryTests
./gradlew test -Dtest.single=Main

#### All Tests
./gradlew test

#### Performance Test
./gradlew test -Dtest.single=TestPerformance

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