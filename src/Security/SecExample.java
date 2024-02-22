package Security;

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;

public class SecExample {
    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        factory.setPort(5671);
        factory.useSslProtocol();

        // Disable server certificate trust verification
        factory.useSslProtocol();

        // Establish the connection
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        // Declare a non-durable, exclusive, auto-delete queue
        channel.queueDeclare("rabbitmq-java-test", false, true, true, null);

        // Publish a message
        channel.basicPublish("", "rabbitmq-java-test", null, "Hello, world".getBytes());

        // Close the channel and connection
        channel.close();
        connection.close();
    }
}
