package Membership;

import Connection.MulticastConnection;
import Storage.MembershipLog;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.concurrent.ThreadPoolExecutor;

public class MembershipProtocolDispatcher implements Runnable {
    private final MulticastConnection connection;
    private final ThreadPoolExecutor executor;
    private MembershipView membershipView;

    MembershipProtocolDispatcher(MulticastConnection connection, ThreadPoolExecutor executor,
                                 MembershipView membershipView) {
        this.connection = connection;
        this.executor = executor;
        this.membershipView = membershipView;
    }
    @Override
    public void run() {
        while(!connection.isClosed()) {
            String receivedMessage;
            try {
                System.out.println("waiting for messages");
                receivedMessage = connection.receive();
                System.out.println("MESSAGE: b\"\"\"\n" + receivedMessage + "\n\"\"\"");
            } catch (SocketTimeoutException e) {
                System.out.println("timedout");
                continue;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            executor.submit(new MembershipProtocolHandler(receivedMessage, membershipView));
        }
    }
}
