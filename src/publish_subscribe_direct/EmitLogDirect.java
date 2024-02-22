package publish_subscribe_direct;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;

import java.util.Arrays;

public class EmitLogDirect {
    private static final String EXCHANGE_NAME ="direct_logs";

    public static void main(String[] argv) throws Exception{
        ConnectionFactory factory= new ConnectionFactory();
        factory.setHost("localhost");

        try(Connection connection= factory.newConnection(); Channel channel= connection.createChannel()) {
            channel.exchangeDeclare(EXCHANGE_NAME, "direct");

            String severity= getSeverity(argv);
            String message= getMessage(argv);

            channel.basicPublish(EXCHANGE_NAME, severity, null, message.getBytes("UTF-8"));
            System.out.println(" [x] Sent '" + severity + "':'" + message + "'");
        }
    }

    private static String getSeverity(String[] args) {
        if (args.length < 1) {
            System.err.println("Usage: EmitLogDirect [info] [warning] [error]");
            System.exit(1);
        }
        return args[0];
    }

    private static String getMessage(String[] args) {
        if (args.length < 2) {
            return "Default message";
        }
        return String.join(" ", Arrays.copyOfRange(args, 1, args.length));
    }
}
