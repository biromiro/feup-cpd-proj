package Membership;

import Connection.MulticastConnection;
import Message.MembershipMessageProtocol;
import Message.MessageProtocolException;

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
                System.out.println("MESSAGE: b\"\"\"\n" + receivedMessage + "\n\"\"\"");
                MembershipMessageProtocol parsedMessage = MembershipMessageProtocol.parse(receivedMessage);
                if (parsedMessage instanceof MembershipMessageProtocol.JoinMessage joinMessage) {
                    System.out.println("received join message on port "
                            + joinMessage.getPort() + " with counter " + joinMessage.getMembershipCounter());
                    // TODO do something with join message

                } else if (parsedMessage instanceof MembershipMessageProtocol.LeaveMessage leaveMessage) {
                    System.out.println("received leave message with counter " + leaveMessage.getMembershipCounter());
                } else if (parsedMessage instanceof MembershipMessageProtocol.MembershipMessage membershipMessage) {
                    System.out.println("received membership message");
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            } catch (MessageProtocolException e) {
                throw new RuntimeException("Invalid message", e);
            }
        }
    }
}
