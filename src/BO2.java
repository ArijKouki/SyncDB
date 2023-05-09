import com.rabbitmq.client.*;
import java.sql.*;
import java.util.concurrent.*;

public class BO2 {
    public static void main(String[] args) {
        // Synchronize once a day
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        scheduler.scheduleAtFixedRate(() -> runMain(), 0, 1, TimeUnit.DAYS);
    }
        private static void runMain() {
        try {
            // Set up MySQL database connection
            Class.forName("com.mysql.cj.jdbc.Driver");
            java.sql.Connection con = DriverManager.getConnection(
                    "jdbc:mysql://localhost:3306/bo2", "root", ""
            );

            // Set up RabbitMQ connection and channel
            String QUEUE_NAME = "BO2_queue";
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost("localhost");
            com.rabbitmq.client.Connection connection = factory.newConnection();
            Channel channel = connection.createChannel();
            // Make queue persistent
            boolean durable = true;
            channel.queueDeclare(QUEUE_NAME, durable, false, false, null);

            // Retrieve data from MySQL database
            Statement statement = con.createStatement();
            ResultSet resultSet = statement.executeQuery("select * from product_sales  where sent = 0");
            while (resultSet.next()) {

                String message =
                        resultSet.getString("product") + "," +
                                resultSet.getString("qty") + "," +
                                resultSet.getString("cost") + "," +
                                resultSet.getString("amt") + "," +
                                resultSet.getString("tax") + "," +
                                resultSet.getString("total") + "," +
                                resultSet.getString("date") + "," +
                                resultSet.getString("region") ;
                System.out.println(message);
                //publish row in queue
                AMQP.BasicProperties props = MessageProperties.PERSISTENT_TEXT_PLAIN;
                channel.basicPublish("", QUEUE_NAME, props, message.getBytes("UTF-8"));

                //update sent flag to 1
                int id = resultSet.getInt("id");
                PreparedStatement updateStmt = con.prepareStatement("UPDATE product_sales SET sent = 1 WHERE id = ?");
                updateStmt.setInt(1, id);
                updateStmt.executeUpdate();
                updateStmt.close();

            }
            //close sql connection
            resultSet.close();
            statement.close();
            con.close();
            //close rabbitmq
            channel.close();
            connection.close();
        } catch (Exception e) {
            System.out.println(e);
        }
    }
}