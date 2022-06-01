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
        System.out.println("Waiting for messages");
        while(!connection.isClosed()) {
            try {
                String receivedMessage = connection.receive();
                executor.submit(
                        new MembershipProtocolHandler(receivedMessage, membershipView, executor, membershipHandler));
            } catch (SocketTimeoutException e) {
                membershipHandler.tryToAssumeMulticasterRole();

                //membershipView.incrementBroadcasterIndex();
                //if (membershipView.isBroadcaster() && !membershipView.isBroadcasting()) {
                ///    membershipHandler.sendBroadcastMembership();
                //}
            } catch (IOException e) {
                e.printStackTrace();
                return;
            }
        }
    }
}
