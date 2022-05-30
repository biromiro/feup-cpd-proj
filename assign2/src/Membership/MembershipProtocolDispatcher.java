package Membership;

import Connection.MulticastConnection;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.concurrent.ThreadPoolExecutor;

public class MembershipProtocolDispatcher implements Runnable {
    private final MulticastConnection connection;
    private final ThreadPoolExecutor executor;
    private MembershipView membershipView;

    private final MembershipHandler membershipHandler;

    MembershipProtocolDispatcher(MembershipHandler membershipHandler, MulticastConnection connection, ThreadPoolExecutor executor,
                                 MembershipView membershipView) {
        this.connection = connection;
        this.executor = executor;
        this.membershipView = membershipView;
        this.membershipHandler = membershipHandler;
    }
    @Override
    public void run() {
        System.out.println("waiting for messages");
        while(!connection.isClosed()) {
            String receivedMessage;
            try {
                receivedMessage = connection.receive();
                System.out.println("MESSAGE: b\"\"\"\n" + receivedMessage + "\n\"\"\"");
            } catch (SocketTimeoutException e) {
                System.out.println("timedout");
                membershipView.setPriority(membershipView.getPriority() - 1);
                if (membershipView.getPriority() == 0) {
                    membershipHandler.sendBroadcastMembership();
                }
                continue;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            System.out.println("Gonna dispatch it");
            executor.submit(new MembershipProtocolHandler(receivedMessage, membershipView));
        }
    }
}
