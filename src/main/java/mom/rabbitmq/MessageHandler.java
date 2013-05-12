package mom.rabbitmq;

/**
 * @author pestrella.developer@gmail.com
 */
public interface MessageHandler {
    String getName();
    void handle(byte[] message);
}
