package Membership;

import Connection.AsyncTcpConnection;
import Message.MembershipMessageProtocol;

import java.io.IOException;
import java.util.concurrent.ThreadPoolExecutor;

public class MembershipMessageDispatcher implements Runnable{
    private final int port;
    private final ThreadPoolExecutor executor;
    private final MembershipView membershipView;
    private String host;

    MembershipMessageDispatcher(ThreadPoolExecutor executor, MembershipView membershipView, String host, int port) {
        this.executor = executor;
        this.membershipView = membershipView;
        this.host = host;
        this.port = port;
    }

    @Override
    public void run() {
        System.out.println("Sending membership view to " + host + ":" + port);
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
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void failed(Throwable exc) {
                throw new RuntimeException("Failed to write to socket", exc);
            }
        });
    }
}
