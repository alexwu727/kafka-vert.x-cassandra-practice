public class LogConsumerImplementation extends BaseLogConsumer{
    public static void main(String[] args) {
        LogConsumerImplementation c = new LogConsumerImplementation();
        c.setConfig();
        c.setProps();
        c.consume();
    }
}
