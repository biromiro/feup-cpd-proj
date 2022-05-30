package Membership;

import Connection.MulticastConnection;
import Message.MembershipLog;
import Message.MembershipMessageProtocol;
import Message.MessageProtocolException;

import java.io.IOException;
import java.nio.channels.AsynchronousSocketChannel;
import java.util.concurrent.ThreadPoolExecutor;

public class MembershipProtocolDispatcher implements Runnable {
    private final MulticastConnection connection;
    private final ThreadPoolExecutor executor;
    private MembershipLog membershipLog;

    MembershipProtocolDispatcher(MulticastConnection connection, ThreadPoolExecutor executor,
                                 MembershipLog membershipLog) {
        this.connection = connection;
        this.executor = executor;
        this.membershipLog = membershipLog;
    }
    @Override
    public void run() {
        while(!connection.isClosed()) {
            String receivedMessage;
            try {
                System.out.println("waiting for messages");
                receivedMessage = connection.receive();
                System.out.println("MESSAGE: b\"\"\"\n" + receivedMessage + "\n\"\"\"");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            executor.submit(new MembershipProtocolHandler(receivedMessage, membershipLog));
        }
    }
}
