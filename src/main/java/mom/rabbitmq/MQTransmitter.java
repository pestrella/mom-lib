package mom.rabbitmq;

import static com.rabbitmq.client.MessageProperties.*;

import com.rabbitmq.client.*;

import org.slf4j.*;

import java.io.*;

/**
 * @author pestrella.developer@gmail.com
 */
public class MQTransmitter {
    private MQConnection connection;
    private Channel channel;

    private static final Logger log = LoggerFactory.getLogger(MQTransmitter.class);

    public MQTransmitter(MQConnection connection) {
        this.connection = connection;
        try {
            channel = connection.getChannel();
        } catch (IOException e) {
            throw new RuntimeException("Unable to establish channel.", e);
        }
    }

    public void send(String exchange, String message) throws IOException {
        while (true) {
            try {
                if (!channel.isOpen())
                    channel = connection.getChannel();

                /* TODO: should there be one channel per queue?
                 * TODO: should we wait for Publisher Confirms?
                 */
                channel.exchangeDeclare(exchange, "direct", true);
                channel.basicPublish(exchange, "", PERSISTENT_TEXT_PLAIN, message.getBytes());
                return;

            } catch (Exception e) {
                log.debug("Retry send message: ({}), exchange={}.", message, exchange);
                /* TODO: apply max retry */
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException interrupt) {}
            }
        }
    }

    /* TODO: should we close the channel when we don't need it anymore? e.g. application shutdown. */
}
