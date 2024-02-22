package read_without_delivercallback;

import com.rabbitmq.client.*;

public class ReadHandleDelivery {
    private final static String QUEUE_NAME = "hello";

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.queueDeclare(QUEUE_NAME, false, false, false, null);

        // Create a new consumer
        DefaultConsumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope,
                                       AMQP.BasicProperties properties, byte[] body) throws java.io.IOException {
                String message = null;
                message = new String(body, "UTF-8");
                System.out.println("Received: " + message);
            }
        };

        // Start consuming messages
        channel.basicConsume(QUEUE_NAME, true, consumer);
    }
}
