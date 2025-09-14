package class4;

public class UserBehavior {
    public String userId;
    public String itemId;
    public String categoryId;
    public String behavior;
    public long timestamp;

    public UserBehavior(String userId, String itemId, String categoryId, String behavior, long timestamp) {
        this.userId = userId;
        this.itemId = itemId;
        this.behavior = behavior;
        this.categoryId = categoryId;
        this.timestamp = timestamp;
    }

    public UserBehavior() {
    }

    @Override
    public String toString() {
        return "UserBehavior{" +
                "userId='" + userId + '\'' +
                ", itemId='" + itemId + '\'' +
                ", categoryId='" + categoryId + '\'' +
                ", behavior='" + behavior + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
