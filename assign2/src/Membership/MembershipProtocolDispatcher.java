package Membership;

import Connection.MulticastConnection;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class MembershipProtocolDispatcher implements Runnable {
    private static final int WAIT_OTHERS_DELAY_MILLISECONDS = 200;
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
            try {
                String receivedMessage = connection.receive();
                executor.submit(new MembershipProtocolHandler(receivedMessage, membershipView, executor,
                        membershipHandler));
            } catch (SocketTimeoutException e) {
                int delay = membershipView.getIndexInCluster() * WAIT_OTHERS_DELAY_MILLISECONDS;
                membershipHandler.sendBroadcastMembership(delay);

                //membershipView.incrementBroadcasterIndex();
                //if (membershipView.isBroadcaster() && !membershipView.isBroadcasting()) {
                ///    membershipHandler.sendBroadcastMembership();
                //}
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
