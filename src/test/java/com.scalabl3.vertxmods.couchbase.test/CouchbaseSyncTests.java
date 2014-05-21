
import static org.junit.Assert.assertEquals;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.Ignore;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import java.io.Closeable;
import java.io.IOException;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.vertx.groovy.core.eventbus.EventBus;
import org.vertx.java.core.Handler;
import org.vertx.java.core.json.JsonObject;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.vertx.java.core.Handler;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.platform.Verticle;
//import org.vertx.java.test.TestModule;
//import org.vertx.java.test.VertxConfiguration;
//import org.vertx.java.test.junit.VertxJUnit4ClassRunner;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: jasdeep
 * Date: 6/8/13
 * Time: 9:00 AM
 * To change this template use File | Settings | File Templates.
 */
//@RunWith(VertxJUnit4ClassRunner.class)
//@VertxConfiguration
//@TestModule(name = "com.scalabl3.vertxmods.couchbase.sync")
public class CouchbaseSyncTests {

    private static EventBus eb;

    private void println(String string) {
        System.out.println(string);
    }

    @Before
    public void setUp() {
        this.println("@Before setUp");
        //EventBus eb = vertx.eventBus();
    }

    @After
    public void tearDown() throws IOException {
        this.println("@After tearDown");

    }

    @Test
    public void test1() {
        this.println("@Test test1()");
    }

    @Test
    public void test2() {
        this.println("@Test test2()");
    }
}

