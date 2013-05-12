package mom.rabbitmq;

import static java.lang.String.*;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.*;

import org.slf4j.*;

import java.io.*;
import java.util.concurrent.*;

/**
 * @author pestrella.developer@gmail.com
 */
public class MQReceiver {
    private final MQConnection connection;
    private final String exchange;
    private final String queue;
    private final MessageHandler handler;
    private final boolean autoAck;

    private final ExecutorService executor;

    private Channel channel;

    private static Logger log = LoggerFactory.getLogger(MQReceiver.class);

    public MQReceiver(MQConnection connection, String exchange, String queue, MessageHandler handler,
            int maxThreads, boolean autoAck) {

        this.connection = connection;
        this.exchange = exchange;
        this.queue = queue;
        this.handler = handler;
        this.autoAck = autoAck;

        this.executor = initThreadPoolExecutor(maxThreads);
    }

    private static ExecutorService initThreadPoolExecutor(int maxThreads) {
        /* We don't want or need a large waiting queue. If all threads are busy and the waiting queue
         * is full, then abort and let the caller handle the rejection. For example, incoming messages
         * may be re-delivered during max capacity.
         */
        ArrayBlockingQueue<Runnable> waitingQueue = new ArrayBlockingQueue<Runnable>(maxThreads / 2);
        ThreadPoolExecutor.AbortPolicy abortPolicy = new ThreadPoolExecutor.AbortPolicy();
        return new ThreadPoolExecutor(1, maxThreads, 30, TimeUnit.SECONDS, waitingQueue, abortPolicy);
    }

    public void listen() {
        channel = establishChannel();

        try {
            channel.basicConsume(queue, autoAck, handler.getName(), new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope,
                    BasicProperties properties, byte[] body) throws IOException {

                    long deliveryTag = envelope.getDeliveryTag();

                    try {
                        executor.execute(new Handler(body, deliveryTag, consumerTag, channel));

                    } catch (RejectedExecutionException e) {
                        try {
                            channel.basicReject(deliveryTag, true);
                            log.error(format(
                                "Too busy to handle message: (%s), exchange=%s, consumer=%s. Marked for redelivery.",
                                new String(body), exchange, consumerTag), e);

                        } catch (IOException ioe) {
                            log.error(format(
                                "Rejected message could not be redelivered: (%s), exchange=%s, consumer=%s",
                                new String(body), exchange, consumerTag), ioe);
                        }
                    }
                }

                @Override
                public void handleShutdownSignal(String consumerTag, ShutdownSignalException e) {
                    if (e.isInitiatedByApplication()) {
                        log.debug(format("Consumer shutdown initiated by application (exchange=%s, consumer=%s).", exchange, consumerTag));
                        return;
                    }
                    log.error(format("Consumer unexpectedly shutdown: exchange=%s, consumer=%s, reason=%s",
                        exchange, consumerTag, e.getReason()), e);
                }
            });
        } catch (IOException e) {
            throw new RuntimeException(format(
                "Consumer could not connect to channel: (%s), exchange=%s, handler=%s",
                channel.getChannelNumber(), exchange, handler.getName()), e);

        }
        log.info("Receiver 'hello?': exchange={}, handler={}", exchange, handler.getName());
    }

    private Channel establishChannel() {
        try {
            Channel channel = connection.getChannel();
            channel.exchangeDeclare(exchange, "direct", true);

            channel.queueDeclare(queue, true, false, false, null);
            channel.queueBind(queue, exchange, "");

            /* TODO: unlimited unack'd deliveries; sure about that? */
            channel.basicQos(0);
            return channel;

        } catch (IOException e) {
            throw new RuntimeException(format(
                "Failed to establish a channel: exchange=%s, handler=%s", exchange, handler.getName()), e);
        }
    }

    /* Handler runnable delegates to the configured MessagesHandler */
    private class Handler implements Runnable {
        final byte[] message;
        final long deliveryTag;
        final String consumerTag;
        final Channel channel;

        Handler(byte[] message, long deliveryTag, String consumerTag, Channel channel) {
            this.message = message;
            this.deliveryTag = deliveryTag;
            this.consumerTag = consumerTag;
            this.channel = channel;
        }

        @Override
        public void run() {
            try {
                handler.handle(message);

            } catch (Exception e) {
                /* will likely not recover from an application error even on re-delivery */
                log.error(format("Unexpected error while handling message: (%s), exchange=%s, consumer=%s",
                    string(message), exchange, consumerTag), e);
            }

            try {
                if (!autoAck)
                    channel.basicAck(deliveryTag, false);

            } catch (Exception e) {
                log.error(format(
                    "Message handling complete, but could not acknowledge message: (%s), channel=%s, exchange=%s, consumer=%s",
                    string(message), channel.getChannelNumber(), exchange, consumerTag), e);
            }
        }

        private String string(byte[] text) {
            try {
                return new String(message, "UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void hangup() {
        if (channel == null)
            return;

        try {
            if (channel.isOpen()) {
                log.debug("Closing channel...");
                channel.close();
            }
        } catch (IOException e) {
            log.error(format("Failed to close channel: (%s), exchange=%s, handler=%s.",
                channel.getChannelNumber(), exchange, handler.getName()), e);
        }
        log.info("Receiver 'bye-bye!': exchange={}, hanlder={}", exchange, handler.getName());
    }
}
