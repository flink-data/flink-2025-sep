package class1;

public class Event {

    public String url;
    public String user;
    public Long timestamp;

    public Event(String url, String user, Long timestamp) {
        this.url = url;
        this.user = user;
        this.timestamp = timestamp;
    }

    public Event() {
    }

    @Override
    public String toString() {
        return "Event{" +
                "url='" + url + '\'' +
                ", user='" + user + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
