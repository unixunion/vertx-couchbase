/**
 * Created with IntelliJ IDEA.
 * User: jasdeep
 * Date: 5/18/13
 * Time: 3:40 PM
 * To change this template use File | Settings | File Templates.
 */
public class User extends ModelBase {

    private String first_name;
    private String last_name;

    public User(String fname, String lname) {
        this.first_name = fname;
        this.last_name = lname;
    }

    @Override
    public String toString() {
        return first_name + " " + last_name;
    }
}