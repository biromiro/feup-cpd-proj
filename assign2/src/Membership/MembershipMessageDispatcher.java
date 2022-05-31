package Membership;

import Connection.AsyncTcpConnection;
import Message.MembershipMessageProtocol;

import java.io.IOException;
import java.util.concurrent.ThreadPoolExecutor;

public class MembershipMessageDispatcher implements Runnable{
    private final int port;
    private final ThreadPoolExecutor executor;
    private final MembershipView membershipView;

    MembershipMessageDispatcher(ThreadPoolExecutor executor, MembershipView membershipView, int port) {
        this.executor = executor;
        this.membershipView = membershipView;
        this.port = port;
    }

    @Override
    public void run() {
        AsyncTcpConnection connection = new AsyncTcpConnection(executor, port);
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
