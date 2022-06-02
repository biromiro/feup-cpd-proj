package Membership;

import Connection.MulticastConnection;

import java.io.IOException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.concurrent.ScheduledThreadPoolExecutor;

public class MembershipProtocolDispatcher implements Runnable {
    private final MulticastConnection connection;
    private final ScheduledThreadPoolExecutor executor;
    private final MembershipView membershipView;
    private final MembershipHandler membershipHandler;

    MembershipProtocolDispatcher(MembershipHandler membershipHandler, MulticastConnection connection,
                                 ScheduledThreadPoolExecutor executor, MembershipView membershipView) {
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
            } catch (SocketException e) {
                if (!connection.isClosed()) {
                    e.printStackTrace();
                    throw new RuntimeException(e.getMessage());
                }
            } catch (IOException e) {
                e.printStackTrace();
                return;
            }
        }
    }
}
