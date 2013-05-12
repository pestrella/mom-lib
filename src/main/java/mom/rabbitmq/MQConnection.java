package mom.rabbitmq;

import static org.slf4j.LoggerFactory.*;

import com.rabbitmq.client.*;

import org.slf4j.*;

import java.io.*;

/**
 * @author pestrella.developer@gmail.com
 */
public class MQConnection {
    private ConnectionFactory factory;
    private Connection connection;

    private static final Logger log = getLogger(MQConnection.class);

    public MQConnection(String host) {
        factory = new ConnectionFactory();
        factory.setHost(host);
        start();
    }

    public void start() {
        try {
            if (connection == null || !connection.isOpen()) {
                log.info("Starting connection...");
                connection = factory.newConnection();
            }
        } catch (IOException e) {
            throw new RuntimeException("Unable to establish connection.", e);
        }
    }

    public void addShutdownListener(ShutdownListener listener) {
        connection.addShutdownListener(listener);
    }

    public Channel getChannel() throws IOException {
        if (!connection.isOpen())
            throw new RuntimeException("Connection not open.");

        return connection.createChannel();
    }

    public void shutdown() {
        log.info("Shutting down connection...");
        if (connection != null) {
            try {
                connection.close();
            } catch (IOException e) {
                throw new RuntimeException("Unable to close connection.", e);
            }
        }
    }
}
