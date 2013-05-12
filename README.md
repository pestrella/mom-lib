MOM Library
===========
A Java client library for RabbitMQ.

Basic Usage
----------
1. Initialize the receiver with your message handler, and start listening:

        MQConnection connection = new MQConnection("localhost");
        MQTransmitter transmitter = new MQTransmitter(connection);
        MyHandler handler = new MyHandler();
        MQReceiver receiver = new MQReceiver(connection, "my_exchange", "my_queue", handler, 10, false);
        receiver.listen();

2. Transmit your message:

        transmitter.send("my_exchange", "Yo dude!");

3. Hang up when your application shuts down:

        receiver.hangup();

Refer to tests for full example.
