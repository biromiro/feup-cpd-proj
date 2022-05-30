package Membership;

import Connection.MulticastConnection;
import Storage.MembershipLog;
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
    private MembershipView membershipView;
    private MulticastConnection clusterConnection;

    private ThreadPoolExecutor executor;

    public MembershipHandler(String mcastAddr, int mcastPort, String nodeId, int storePort,
                             MembershipView membershipView, ThreadPoolExecutor executor) {
        this.mcastAddr = mcastAddr;
        this.mcastPort = mcastPort;
        this.nodeId = nodeId;
        this.storePort = storePort;
        this.membershipView = membershipView;
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
            int counter) throws IOException {
        clusterConnection.send(MembershipMessageProtocol.join(this.nodeId, this.storePort, counter));
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
                executor.submit(new MembershipMessageHandler(worker, membershipView));
            } catch (TimeoutException e) {
                System.out.println("There was a timeout, trying again");
                clusterConnection.send(MembershipMessageProtocol.join(this.nodeId, this.storePort, counter));
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
            connectToCluster(serverSocket, clusterConnection, count);
            System.out.println("Goodbye socket for membership");
        } catch (IOException e) {
            throw new RuntimeException("Failed to connect to async server.", e);
        }
    }

    public void leave(int count) {
        try  {
            clusterConnection.leave();
            clusterConnection.send(MembershipMessageProtocol.leave(this.nodeId, count));
            clusterConnection.close();
            System.out.println("message sent");
        } catch (IOException e) {
            throw new RuntimeException("Failed to connect to multicast group.", e);
        }
    }

    public void receive() {
        executor.submit(new MembershipProtocolDispatcher(clusterConnection, executor, membershipView));
    }
}
