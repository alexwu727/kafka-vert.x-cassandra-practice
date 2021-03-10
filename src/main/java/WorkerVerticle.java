import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;
import org.json.JSONException;
import org.json.JSONObject;

public class WorkerVerticle extends AbstractVerticle {
    public String dataProcessing(String data) throws InterruptedException {
        String newString = "";
        Thread.sleep(3000);
        for(int i = data.length() - 1; i >= 0; i--){
            newString += data.charAt(i);
        }
        return newString;
    }

    @Override
    public void start() throws Exception {
        EventBus eventBus = vertx.eventBus();
        System.out.println("deploy worker verticle "+ deploymentID()+" successfully");
        eventBus.consumer("worker", msg->{
            try {
                System.out.println(msg.body());
                String msg2 = (String) msg.body();
                JSONObject logDataJson = new JSONObject(msg2);
                logDataJson.put("sender_id", dataProcessing(logDataJson.getString("sender_id")));
                System.out.println(logDataJson);
                msg.reply(logDataJson.toString());
            } catch (InterruptedException | JSONException e) {
                System.out.println("error occurred");
            }
        });
    }
}
