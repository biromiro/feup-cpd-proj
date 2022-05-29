package Membership;

import Connection.MulticastConnection;

import java.io.IOException;
import java.nio.channels.AsynchronousSocketChannel;

public class MembershipReceiverHandler implements Runnable {

    private final MulticastConnection connection;

    MembershipReceiverHandler(MulticastConnection connection) {
        this.connection = connection;
    }
    @Override
    public void run() {
        while(!connection.isClosed()) {
            try {
                System.out.println("waiting for messages");
                String receivedMessage = connection.receive();
                System.out.println("oh well b\"\"\"\n" + receivedMessage + "\n\"\"\"");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
