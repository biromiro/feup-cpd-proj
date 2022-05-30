package Membership;

import Connection.MulticastConnection;
import Message.MembershipMessageProtocol;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.util.concurrent.*;

public class MembershipHandler {
    private static final int MAX_MEMBERSHIP_MESSAGES = 3;
    private static final int MEMBERSHIP_ACCEPT_TIMEOUT = 500;
    private static final int MAX_RETRANSMISSION_TIMES = 3;
    private final String mcastAddr;
    private final int mcastPort;
    private final String nodeId;
    private final int storePort;
    private MulticastConnection clusterConnection;

    // TODO why is this unassigned?
    private ThreadPoolExecutor executor;

    public MembershipHandler(String mcastAddr, int mcastPort, String nodeId, int storePort, ThreadPoolExecutor executor) {
        this.mcastAddr = mcastAddr;
        this.mcastPort = mcastPort;
        this.nodeId = nodeId;
        this.storePort = storePort;
        try {
            this.clusterConnection = new MulticastConnection(mcastAddr, mcastPort);
        } catch (IOException e) {
            throw new RuntimeException("Failed to connect to multicast group.", e);
        }
        this.executor = executor;
    }

    private AsynchronousServerSocketChannel initializeServerSocket() throws IOException {
        // TODO probably abstract AsynchronousServerSocketChannel as a substitute to UnicastConnection
        return AsynchronousServerSocketChannel.open().bind(new InetSocketAddress(this.storePort));
    }

    private void connectToCluster(
            AsynchronousServerSocketChannel serverSocket,
            MulticastConnection clusterConnection,
            String message) throws IOException {
        clusterConnection.send(message);
        System.out.println("message sent");

        int transmissionCount = 1;
        int membershipMessagesCount = 0;

        Future<AsynchronousSocketChannel> future = serverSocket.accept();

        AsynchronousSocketChannel worker;
        while (transmissionCount < MAX_RETRANSMISSION_TIMES) {
            try {
                worker = future.get(MEMBERSHIP_ACCEPT_TIMEOUT, TimeUnit.MILLISECONDS);
                if (membershipMessagesCount < MAX_MEMBERSHIP_MESSAGES) {
                    future = serverSocket.accept();
                } else break;

                membershipMessagesCount += 1;
                executor.submit(new MembershipMessageHandler(worker));
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
        try (AsynchronousServerSocketChannel serverSocket = initializeServerSocket()) {
            System.out.println("Server is listening on port " + this.storePort);
            if (clusterConnection.isClosed()) clusterConnection = new MulticastConnection(mcastAddr, mcastPort);
            String joinMessage = MembershipMessageProtocol.join(this.nodeId, this.storePort, count);
            connectToCluster(serverSocket, clusterConnection, joinMessage);
            System.out.println("Goodbye socket for membership");
        } catch (IOException e) {
            throw new RuntimeException("Failed to connect to async server.", e);
        }
    }

    public void leave(int count) {
        try  {
            clusterConnection.send(MembershipMessageProtocol.leave(this.nodeId, count));
            clusterConnection.close();
            System.out.println("message sent");
        } catch (IOException e) {
            throw new RuntimeException("Failed to connect to multicast group.", e);
        }
    }

    public void receive(ThreadPoolExecutor executor, MembershipView membershipView) {
        executor.submit(new MembershipProtocolDispatcher(this, clusterConnection, executor, membershipView));
    }

    public void reinitialize() {
        // TODO send multicast saying a crash occurred, asking for 3 membership logs
        try (AsynchronousServerSocketChannel serverSocket = initializeServerSocket()) {
            System.out.println("Server is listening on port " + this.storePort);
            if (clusterConnection.isClosed()) clusterConnection = new MulticastConnection(mcastAddr, mcastPort);
            String reinitializeMessage = MembershipMessageProtocol.reinitialize(this.nodeId, this.storePort);
            connectToCluster(serverSocket, clusterConnection, reinitializeMessage);
            System.out.println("Goodbye socket for membership");
        } catch (IOException e) {
            throw new RuntimeException("Failed to connect to async server upon reinitialize message.", e);
        }
    }

    public void sendBroadcastMembership(ThreadPoolExecutor executor, MembershipView membershipView) {
        membershipView.setPriority(0);
        executor.submit(new MembershipEchoMessageSender(executor, clusterConnection, membershipView));
    }
}
