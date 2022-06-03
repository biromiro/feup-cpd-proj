package Membership;

import Connection.AsyncTcpConnection;
import Message.MembershipMessageProtocol;

import java.io.IOException;
import java.util.concurrent.ScheduledThreadPoolExecutor;

public class MembershipMessageDispatcher implements Runnable{
    private final int port;
    private final ScheduledThreadPoolExecutor executor;
    private final MembershipView membershipView;
    private final String host;

    MembershipMessageDispatcher(ScheduledThreadPoolExecutor executor, MembershipView membershipView, String host, int port) {
        this.executor = executor;
        this.membershipView = membershipView;
        this.host = host;
        this.port = port;
    }

    @Override
    public void run() {
        AsyncTcpConnection.connect(executor, host, port, new AsyncTcpConnection.ConnectionHandler() {
            @Override
            public void completed(AsyncTcpConnection connection) {
                sendMembership(connection);
            }

            @Override
            public void failed(Throwable exc) {
                System.out.println(exc.getMessage());
            }
        });
    }

    private void sendMembership(AsyncTcpConnection connection) {
        connection.write(MembershipMessageProtocol.membership(membershipView), new AsyncTcpConnection.WriteHandler() {
            @Override
            public void completed(Integer result) {
                try {
                    connection.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            @Override
            public void failed(Throwable exc) {
                exc.printStackTrace();
            }
        });
    }
}
