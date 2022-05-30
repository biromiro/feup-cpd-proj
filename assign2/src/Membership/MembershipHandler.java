package Membership;

import Connection.MulticastConnection;
import Message.MembershipLog;
import Message.MembershipMessageProtocol;
import Storage.PersistentStorage;

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

    // TODO why is this unassigned?
    private ThreadPoolExecutor executor;

    public MembershipHandler(String mcastAddr, int mcastPort, String nodeId, int storePort) {
        this.mcastAddr = mcastAddr;
        this.mcastPort = mcastPort;
        this.nodeId = nodeId;
        this.storePort = storePort;
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
                executor.submit(new MembershipMessageHandler(worker));
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

            try (MulticastConnection clusterConnection = new MulticastConnection(mcastAddr, mcastPort)) {
                connectToCluster(serverSocket, clusterConnection, count);
            } catch (IOException e) {
                throw new RuntimeException("Failed to connect to multicast group.", e);
            }

            System.out.println("Goodbye socket for membership");
        } catch (IOException e) {
            throw new RuntimeException("Failed to connect to async server.", e);
        }
    }

    public void leave(int count) {
        try (MulticastConnection clusterConnection = new MulticastConnection(mcastAddr, mcastPort)) {
            clusterConnection.send(MembershipMessageProtocol.leave(this.nodeId, count));
            System.out.println("message sent");
        } catch (IOException e) {
            throw new RuntimeException("Failed to connect to multicast group.", e);
        }
    }

    public void receive(ThreadPoolExecutor executor, MembershipLog membershipLog) {
        MulticastConnection clusterConnection = null;
        try {
            clusterConnection = new MulticastConnection(mcastAddr, mcastPort);
        } catch (IOException e) {
            throw new RuntimeException("Failed to connect to multicast group.", e);
        }

        executor.submit(new MembershipProtocolDispatcher(clusterConnection, executor, membershipLog));
    }
}
