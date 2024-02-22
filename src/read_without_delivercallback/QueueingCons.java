package read_without_delivercallback;

import com.rabbitmq.client.*;

public class QueueingCons {

    private final static String QUEUE_NAME = "hello";
    private final static String EXCHANGE_NAME = "logs";
    private static final String ROUTING_KEY = "info";

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
        channel.exchangeDeclare(EXCHANGE_NAME, "direct", true);
        channel.queueBind(QUEUE_NAME, EXCHANGE_NAME, ROUTING_KEY);
        QueueingConsumer consumer = new QueueingConsumer(channel);
        channel.basicConsume(QUEUE_NAME, false, consumer);

        while (true) {
            channel.txSelect();
            QueueingConsumer.Delivery delivery = consumer.nextDelivery();
            String message = new String(delivery.getBody());
            if (delivery.getProperties().getContentType() != null) {
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                channel.txCommit();
            } else {
                channel.txRollback();
            }
        }

    }
}
