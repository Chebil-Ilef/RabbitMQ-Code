package acknowledgement_management;

import com.rabbitmq.client.*;

import java.io.IOException;

public class multipleAck {
    private final static String QUEUE_NAME = "my_queue";

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {
            boolean autoAck = false;
            channel.basicConsume(QUEUE_NAME, autoAck, "a-consumer-tag", new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                    long deliveryTag = envelope.getDeliveryTag();
                    try {
                        // Process the received message
                        String message = new String(body, "UTF-8");
                        System.out.println("Received message: " + message);

                        // Acknowledge the delivery
                        channel.basicAck(deliveryTag, true); // Acknowledge multiple deliveries
                    } catch (Exception e) {
                        // Handle any processing errors
                        e.printStackTrace();

                        // Reject and requeue the message
                        try {
                            channel.basicReject(deliveryTag, true);
                        } catch (IOException ex) {
                            ex.printStackTrace();
                        }
                    }
                }
            });

        }
    }
}
