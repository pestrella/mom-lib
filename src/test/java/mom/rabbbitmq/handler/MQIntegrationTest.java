package mom.rabbbitmq.handler;

import static java.lang.String.*;
import static java.util.concurrent.TimeUnit.*;
import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;
import mom.rabbitmq.*;

import org.junit.*;

import java.io.*;
import java.util.concurrent.*;

/**
 * @author pestrella.developer@gmail.com
 */
public class MQIntegrationTest {
    private MQConnection connection;
    private MQTransmitter transmitter;
    private MQReceiver receiver;

    private final String exchange = "mom.test";
    private final String queue = "test_queue";
    private final int maxWorkerThreads = 1000;

    private EchoHandler handler;
    private boolean autoAck = false;

    @Before
    public void setup() {
        connection = new MQConnection("localhost");
        transmitter = new MQTransmitter(connection);
        handler = new EchoHandler();
        receiver = new MQReceiver(connection, exchange, queue, handler, maxWorkerThreads, autoAck);
    }

    @After
    public void tearDown() {
        receiver.hangup();
        connection.shutdown();
    }

    @Test
    public void testSingleMessage() throws IOException, InterruptedException {
        double tag = Math.random();
        String message = format("What's up? [test=%s]", tag);

        CountDownLatch messageReceivedLatch = new CountDownLatch(1);
        handler.setMessageReceivedLatch(messageReceivedLatch);
        handler.resetCount();

        receiver.listen();
        transmitter.send(exchange, message);

        /* give receiver time to finish up */
        messageReceivedLatch.await(500, MILLISECONDS);

        assertThat(handler.getLastMessage(), equalTo(message));
        assertThat(handler.getMessagesProcessedCount(), equalTo(1));
    }
}
