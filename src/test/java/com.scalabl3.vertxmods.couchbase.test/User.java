/**
 * Created with IntelliJ IDEA.
 * User: jasdeep
 * Date: 5/18/13
 * Time: 3:40 PM
 * To change this template use File | Settings | File Templates.
 */

package com.scalabl3.vertxmods.couchbase.test;

public class User {

    private String username;
    private String password;

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public User(String username, String password) {
        this.username = username;
        this.password = password;
    }

    @Override
    public String toString() {
        return "UserObject [username=" + username + ", password=" + password + "]";
    }
}