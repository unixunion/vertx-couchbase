package com.scalabl3.vertxmods.couchbase.sync; /**
 * Created with IntelliJ IDEA.
 * User: jasdeep
 * Date: 5/17/13
 * Time: 1:13 PM
 * To change this template use File | Settings | File Templates.
 */

import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

public final class ParseNodeList {
    private ParseNodeList() {}

    public static List<URI> getAddresses(String s) {
        if (s == null) {
            throw new NullPointerException("Null nodes list");
        }
        if (s.trim().equals("")) {
            throw new IllegalArgumentException("No nodes in list:  ``" + s + "''");
        }
        ArrayList<URI> nodes = new ArrayList<URI>();

        for (String nodelist : s.split("(?:\\s|,)+")) {
            if (nodelist.equals("")) {
                continue;
            }

            int finalColon = nodelist.lastIndexOf(':');
            if (finalColon < 1) {
                throw new IllegalArgumentException("Invalid server ``" + nodelist
                        + "'' in list:  " + s);
            }
            String ip = nodelist.substring(0, finalColon);
            String portNum = nodelist.substring(finalColon + 1);

            nodes.add(URI.create("http://" + ip + ":" + Integer.parseInt(portNum) + "/pools"));
        }
        assert !nodes.isEmpty() : "No nodes were found";
        return nodes;
    }

    public static List<URI> getAddresses(List<String> servers) {

        ArrayList<URI> nodes = new ArrayList<URI>(servers.size());

        for (String server : servers) {

            int finalColon = server.lastIndexOf(':');

            if (finalColon < 1) {
                throw new IllegalArgumentException("Invalid server ``" + server
                        + "'' in list:  " + server);
            }
            String ip = server.substring(0, finalColon);
            String portNum = server.substring(finalColon + 1);

            nodes.add(URI.create("http://" + ip + ":" + Integer.parseInt(portNum) + "/pools"));
        }
        if (nodes.isEmpty()) {
            // servers was passed in empty, and shouldn't have been
            throw new IllegalArgumentException("parameter servers cannot be empty");
        }
        return nodes;
    }

    public static List<URI> getAddressesFromURL(List<URL> servers) {
        ArrayList<URI> nodes =  new ArrayList<URI>(servers.size());

        for (URL server : servers) {
            nodes.add(URI.create("http://" + server.getHost() + ":" + server.getPort() + "/pools"));
        }
        return nodes;
    }
}
