# Vertx.io (Vert.x) Couchbase Message Bus SDK/Wrapper

Message Bus based style of working with Couchbase Server with command message, callbacks and futures depending on operation type/style.
You can instantiate multiple mods of course, and mix the styles (sync/async) depending on what you are trying to achieve.

Two different mods for different styles of operations:

`com.scalabl3.vertxmods.couchbase.async`
`com.scalabl3.vertxmods.couchbase.sync`

I worked on this last in June 2013 and got a lot of it working, but need to refresh it all and see where it was left off.

TODO
---
* Needs updating to gradle style project
* Was created for older vert.x release (2.0.0)
* Full test coverage not there yet
* Needs to be updated to latest Couchbase Java SDK