package header_exchange;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

public class ConnectionManager {
    private static Connection connection = null;

    public static Connection getConnection(){
        if(connection== null){
            try{
                ConnectionFactory connectionFactory= new ConnectionFactory();
                connection = connectionFactory.newConnection("amqp://guest:guest@localhost:5672/");
            }catch(IOException | TimeoutException e){
                e.printStackTrace();
            }
        }
        return connection;
    }

    public static void declareExchange() throws IOException, TimeoutException{
        Channel channel= ConnectionManager.getConnection().createChannel();
        //declare header exchange
        channel.exchangeDeclare("my_header", BuiltinExchangeType.HEADERS, true);
        channel.close();
    }

    public static void declareQueues() throws IOException, TimeoutException{
        Channel channel= ConnectionManager.getConnection().createChannel();
        //declare queues
        channel.queueDeclare("HealthQ", true, false, false, null);
        channel.queueDeclare("SportsQ", true, false, false, null);
        channel.queueDeclare("EducationQ", true, false, false, null);
        channel.close();
    }

    public static void declareBindings() throws IOException, TimeoutException{
        Channel channel= ConnectionManager.getConnection().createChannel();

        Map<String, Object> healthArgs= new HashMap<String, Object>();
        healthArgs.put("x-match", "any");
        healthArgs.put("h1", "Header1");
        healthArgs.put("h2", "Header2");
        channel.queueBind("HealthQ", "my_header", "", healthArgs);

        Map<String, Object> sportsArgs= new HashMap<>();
        sportsArgs.put("x-match", "all");
        sportsArgs.put("h1", "Header1");
        sportsArgs.put("h2", "Header2");
        channel.queueBind("SportsQ", "my_header", "", sportsArgs);

        Map<String, Object> educationArgs= new HashMap<>();
        educationArgs.put("x-match", "all");
        educationArgs.put("h1", "Header1");
        educationArgs.put("h2", "Header2");
        channel.queueBind("EducationQ", "my_header", "", educationArgs);

        channel.close();
    }

    public static void subscribeMessage() throws IOException, TimeoutException{
        Channel channel= ConnectionManager.getConnection().createChannel();
        channel.basicConsume("HealthQ", true,((consumerTag, message) -> {
            System.out.println("\n\n==== Health Queue ====");
            System.out.println(consumerTag);
            System.out.println("HealthQ: " + new String(message.getBody()));
            System.out.println(message.getEnvelope());
        }), consumerTag -> {});

        channel.basicConsume("SportsQ", true,((consumerTag, message) -> {
            System.out.println("\n\n==== Sports Queue ====");
            System.out.println(consumerTag);
            System.out.println("SportsQ: " + new String(message.getBody()));
            System.out.println(message.getEnvelope());
        }), consumerTag -> {});

        channel.basicConsume("EducationQ", true,((consumerTag, message) -> {
            System.out.println("\n\n==== Education Queue ====");
            System.out.println(consumerTag);
            System.out.println("EducationQ: " + new String(message.getBody()));
            System.out.println(message.getEnvelope());
        }), consumerTag -> {});

    }

    public static void publishMessage() throws IOException, TimeoutException{
        Channel channel= ConnectionManager.getConnection().createChannel();

        String message= "Header exchange message";
        Map<String,Object> headerMap= new HashMap<>();
        headerMap.put("h1", "Header1");
        headerMap.put("h3", "Header3");

        AMQP.BasicProperties properties= new AMQP.BasicProperties.Builder().headers(headerMap).build();

        channel.basicPublish("my_header", "", properties, message.getBytes());

        message= "Header exchange message 2";
        headerMap.put("h2", "Header2");
        properties= new AMQP.BasicProperties.Builder().headers(headerMap).build();
        channel.basicPublish("my_header", "", properties, message.getBytes());

        channel.close();

    }

    public static void main(String[] args) throws IOException, TimeoutException{
        ConnectionManager.declareQueues();
        ConnectionManager.declareExchange();
        ConnectionManager.declareBindings();

        //threads created to publish subscribe asynchronous
        Thread subscribeThread= new Thread(){
            public void run(){
                try{
                    ConnectionManager.subscribeMessage();
                }catch(IOException | TimeoutException e){
                    e.printStackTrace();
                }
            }
        };

        Thread publishThread= new Thread(){
            public void run(){
                try{
                    ConnectionManager.publishMessage();
                }catch(IOException | TimeoutException e){
                    e.printStackTrace();
                }
            }
        };
    }
}
