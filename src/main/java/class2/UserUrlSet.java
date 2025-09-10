package class2;

import java.util.Set;

public class UserUrlSet {

    public String user;
    public Set<String> urlSet;

    public UserUrlSet(String user, Set<String> urlSet) {
        this.user = user;
        this.urlSet = urlSet;
    }

    public UserUrlSet() {
    }

    @Override
    public String toString() {
        return "UserUrlSet{" +
                "user='" + user + '\'' +
                ", urlSet=" + urlSet +
                '}';
    }
}
