package class1;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Calendar;
import java.util.Random;

public class ClickSource implements SourceFunction<Event> {
    private Boolean isRunning = true;
    @Override
    public void run(SourceContext<Event> ctx) throws Exception {
        Random random = new Random();
        String[] users = {"Bob", "Nancy", "Kyle", "Tom", "Kevin"};
        String[] urls = {"fav/", "order/", "cart/", "payment/", "referral/", "view/", "exit/", "refresh/", "signup/", "signin/"};
        while (isRunning) {
            String user = users[random.nextInt(users.length)];
            String url = urls[random.nextInt(urls.length)];
            long timestamp = Calendar.getInstance().getTimeInMillis();
            ctx.collect(new Event(url, user, timestamp));
            Thread.sleep(1000L);
        }
    }
    @Override
    public void cancel() {
        isRunning = false;
    }
}
