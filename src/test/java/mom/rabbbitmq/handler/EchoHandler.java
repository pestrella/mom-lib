package mom.rabbbitmq.handler;

import mom.rabbitmq.*;

import org.slf4j.*;

import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * @author pestrella.developer@gmail.com
 */
public class EchoHandler implements MessageHandler {
    private String lastMessage;
    private CountDownLatch messageReceivedLatch;
    private AtomicInteger count = new AtomicInteger();

    private static final Logger log = LoggerFactory.getLogger(EchoHandler.class);

    @Override
    public String getName() {
        return "EchoHandler";
    }

    @Override
    public void handle(byte[] payload) {
        String message = new String(payload);
        int n = count.incrementAndGet();
        log.info("Message received: {}, count: {}", message, n);
        lastMessage =  message;

        if (messageReceivedLatch != null)
            messageReceivedLatch.countDown();
    }

    public void setMessageReceivedLatch(CountDownLatch latch) {
        messageReceivedLatch = latch;
    }

    public void resetCount() {
        count = new AtomicInteger();
    }

    public String getLastMessage() {
        return lastMessage;
    }

    public int getMessagesProcessedCount() {
        return count.get();
    }
}
