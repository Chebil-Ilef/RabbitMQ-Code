package two_files_dispatching;

import com.rabbitmq.client.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class TraitementFilePrio {
    private final static String QUEUE_NAME = "F1";

    public Object deseriablize(byte[] byteArray) throws Exception {
        ByteArrayInputStream in = new ByteArrayInputStream(byteArray);
        ObjectInputStream is = new ObjectInputStream(in);
        return is.readObject();
    }

    public byte[] getByteArray(Object o) throws Exception {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        ObjectOutputStream objout = new ObjectOutputStream(os);
        objout.writeObject(o);
        return os.toByteArray();
    }

    public void LectureFileF1() throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.queueDeclare(QUEUE_NAME, false, false, false, null);

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            byte[] byteArray = delivery.getBody();
            obj o = null;
            try { o = (obj) deseriablize(byteArray);
            } catch (Exception e) { throw new RuntimeException(e); }
            System.out.println(" [x] Received '" + o + "'");
            //dispatching
            if (o.getPrio() == 1) {
                try {
                    sendMsg_Queue("File Prioritaire", o);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
            else {
                try {
                    sendMsg_Queue("File Non Prioritaire", o);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        };
        channel.basicConsume(QUEUE_NAME, true, deliverCallback, consumerTag -> {
        });
    }

    public void sendMsg_Queue(String QUEUE_NAME, obj o) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        try (Connection connection = factory.newConnection(); Channel channel = connection.createChannel()) {
            channel.queueDeclare(QUEUE_NAME, false, false, false, null);
            channel.basicPublish("", QUEUE_NAME, null, getByteArray(o));
            System.out.println(" [x] Sent '" + o + "'");

        }
    }

    public void LectureFileMessagePrioritaire() throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();

        Channel channelPrio = connection.createChannel();
        Channel channelNonPrio = connection.createChannel();

        GetResponse response = channelPrio.basicGet("File Prioritaire", false);
        if (response != null) {
            GetResponse response2 = channelNonPrio.basicGet("File Non Prioritaire", false);
            System.out.println(" [x] Received '" + new String(response.getBody(), "UTF-8") + "'");
        } else {
            System.out.println("[x] Receive non prio " + response.getBody());
        }
    }
}
