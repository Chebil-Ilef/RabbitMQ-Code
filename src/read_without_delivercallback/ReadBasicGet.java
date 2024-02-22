package read_without_delivercallback;

import com.rabbitmq.client.*;

public class ReadBasicGet {
    private final static String QUEUE_NAME = "hello";

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();

        Channel channel = connection.createChannel();
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);

        GetResponse response = channel.basicGet(QUEUE_NAME, false);
        if (response != null) {
            byte[] body = response.getBody();
            String message = new String(body, "UTF-8");
            System.out.println(" [x] Received '" + message + "'");
            channel.basicAck(response.getEnvelope().getDeliveryTag(), false);
        } else {
            System.out.println("No message available");
        }

        channel.close();
        connection.close();
    }
}
