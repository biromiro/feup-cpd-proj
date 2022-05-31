package Membership;

import Connection.MulticastConnection;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.concurrent.ThreadPoolExecutor;

public class MembershipProtocolDispatcher implements Runnable {
    private final MulticastConnection connection;
    private final ThreadPoolExecutor executor;
    private final MembershipView membershipView;
    private final MembershipHandler membershipHandler;

    MembershipProtocolDispatcher(MembershipHandler membershipHandler, MulticastConnection connection,
                                 ThreadPoolExecutor executor, MembershipView membershipView) {
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
                // System.out.println("MESSAGE: b\"\"\"\n" + receivedMessage + "\n\"\"\"");
            } catch (SocketTimeoutException e) {
                membershipView.incrementBroadcasterIndex();
                if (membershipView.isBroadcaster() && !membershipView.isBroadcasting()) {
                    membershipHandler.sendBroadcastMembership();
                }
                continue;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            executor.submit(new MembershipProtocolHandler(receivedMessage, membershipView, executor));
        }
    }
}
