package Membership;

import Connection.AsyncServer;
import Connection.AsyncTcpConnection;
import Connection.MulticastConnection;
import Message.MembershipMessageProtocol;

import java.io.IOException;
import java.nio.channels.AsynchronousSocketChannel;
import java.util.concurrent.*;

public class MembershipHandler {
    private static final int MAX_MEMBERSHIP_MESSAGES = 3;
    private static final int MEMBERSHIP_ACCEPT_TIMEOUT = 1000;
    private static final int MAX_RETRANSMISSION_TIMES = 3;
    private final String mcastAddr;
    private final int mcastPort;
    private final String nodeId;
    private final MembershipView membershipView;
    private MulticastConnection clusterConnection;
    private final ThreadPoolExecutor executor;

    public MembershipHandler(String mcastAddr, int mcastPort, String nodeId,
                             MembershipView membershipView, ThreadPoolExecutor executor) {
        this.mcastAddr = mcastAddr;
        this.mcastPort = mcastPort;
        this.nodeId = nodeId;
        this.membershipView = membershipView;
        try {
            this.clusterConnection = new MulticastConnection(mcastAddr, mcastPort);
        } catch (IOException e) {
            throw new RuntimeException("Failed to connect to multicast group.", e);
        }
        this.executor = executor;
    }

    private void connectToCluster(
            AsyncServer serverSocket,
            MulticastConnection clusterConnection,
            String message) throws IOException {
        clusterConnection.send(message);

        int transmissionCount = 1;
        int membershipMessagesCount = 0;

        Future<AsynchronousSocketChannel> future = serverSocket.accept();
        while (transmissionCount < MAX_RETRANSMISSION_TIMES) {
            try {
                AsynchronousSocketChannel worker = future.get(MEMBERSHIP_ACCEPT_TIMEOUT, TimeUnit.MILLISECONDS);
                if (membershipMessagesCount < MAX_MEMBERSHIP_MESSAGES) {
                    future = serverSocket.accept();
                } else break;

                membershipMessagesCount += 1; // TODO only increment this if message received was actually a MEMBERSHIP
                executor.submit(new MembershipMessageHandler(new AsyncTcpConnection(worker), membershipView));
            } catch (TimeoutException e) {
                System.out.println("There was a timeout, trying again");
                clusterConnection.send(message);
                transmissionCount += 1;
            } catch (InterruptedException ex) {
                System.out.println("Server was not initialized: " + ex.getMessage());
            } catch (ExecutionException ex) {
                System.out.println("Server exception: " + ex.getMessage());
                ex.printStackTrace();
                transmissionCount = MAX_RETRANSMISSION_TIMES;
            }
        }
    }

    public void join(int count) {
        try (AsyncServer serverSocket = new AsyncServer(executor)) {
            if (clusterConnection.isClosed()) clusterConnection = new MulticastConnection(mcastAddr, mcastPort);
            String joinMessage = MembershipMessageProtocol.join(this.nodeId, serverSocket.getPort(), count);
            connectToCluster(serverSocket, clusterConnection, joinMessage);
            //if (membershipView.isBroadcaster() && !membershipView.isBroadcasting()) {
            //    this.sendBroadcastMembership();
            //}

        } catch (IOException e) {
            throw new RuntimeException("Failed to connect to async server.", e);
        }
    }

    public void leave(int count) {
        try  {
            clusterConnection.leave();
            clusterConnection.send(MembershipMessageProtocol.leave(this.nodeId, count));
            membershipView.stopBroadcasting();
            clusterConnection.close();
        } catch (IOException e) {
            throw new RuntimeException("Failed to connect to multicast group.", e);
        }
    }

    public void receive() {
        executor.submit(new MembershipProtocolDispatcher(this, clusterConnection, executor, membershipView));
    }

    public void reinitialize() {
        // TODO send multicast saying a crash occurred, asking for 3 membership logs
        try (AsyncServer serverSocket = new AsyncServer(executor)) {
            if (clusterConnection.isClosed()) clusterConnection = new MulticastConnection(mcastAddr, mcastPort);
            String reinitializeMessage = MembershipMessageProtocol.reinitialize(this.nodeId, serverSocket.getPort());
            connectToCluster(serverSocket, clusterConnection, reinitializeMessage);
            //if (membershipView.isBroadcaster() && !membershipView.isBroadcasting()) this.sendBroadcastMembership();
        } catch (IOException e) {
            throw new RuntimeException("Failed to connect to async server upon reinitialize message.", e);
        }
    }

    public void sendBroadcastMembership() {
        if (!membershipView.isBroadcasting()) {
            membershipView.startBroadcasting();
            executor.submit(new MembershipEchoMessageSender(executor, clusterConnection, membershipView));
        }
    }
}
