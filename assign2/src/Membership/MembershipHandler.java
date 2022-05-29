package Membership;

import Connection.MulticastConnection;
import Message.MembershipMessageProtocol;
import Storage.PersistentStorage;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.util.concurrent.*;

public class MembershipHandler implements MembershipService{
    private static final int MAX_MEMBERSHIP_MESSAGES = 3;
    private static final int MEMBERSHIP_ACCEPT_TIMEOUT = 500;
    private static final int MAX_RETRANSMISSION_TIMES = 3;
    private final PersistentStorage storage;
    private final String mcastAddr;
    private final int mcastPort;
    private final int storePort;

    private ThreadPoolExecutor executor;

    public MembershipHandler(PersistentStorage storage, String mcastAddr, int mcastPort, int storePort, ThreadPoolExecutor executor) {
        this.storage = storage;
        this.mcastAddr = mcastAddr;
        this.mcastPort = mcastPort;
        this.storePort = storePort;
    }

    private AsynchronousServerSocketChannel initializeServerSocket() throws IOException {
        // TODO probably abstract AsynchronousServerSocketChannel as a substitute to UnicastConnection
        return AsynchronousServerSocketChannel.open().bind(new InetSocketAddress(this.storePort));
    }

    private void incrementCounter(MembershipCounter membershipCounter) {
        try {
            membershipCounter.increment();
        } catch (IOException e) {
            throw new RuntimeException("Failed to increment membership counter in non-volatile memory.", e);
        }
    }

    private void connectToCluster(
            AsynchronousServerSocketChannel serverSocket,
            MulticastConnection clusterConnection,
            int counter) throws IOException {
        clusterConnection.send(MembershipMessageProtocol.join(counter, this.storePort));
        System.out.println("message sent");

        int transmissionCount = 1;
        int membershipMessagesCount = 0;

        Future<AsynchronousSocketChannel> future = serverSocket.accept();

        AsynchronousSocketChannel worker = null;
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
                clusterConnection.send(MembershipMessageProtocol.join(counter, this.storePort));
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

    public void join() {
        MembershipCounter membershipCounter = new MembershipCounter(storage);
        if (membershipCounter.isJoining()) {
            System.out.println("Node is already in the cluster.");
            return;
        }
        incrementCounter(membershipCounter);

        try (AsynchronousServerSocketChannel serverSocket = initializeServerSocket()) {
            System.out.println("Server is listening on port " + this.storePort);

            try (MulticastConnection clusterConnection = new MulticastConnection(mcastAddr, mcastPort)) {
                connectToCluster(serverSocket, clusterConnection, membershipCounter.get());
            } catch (IOException e) {
                throw new RuntimeException("Failed to connect to multicast group.", e);
            }

            System.out.println("Goodbye socket for membership");
        } catch (IOException e) {
            throw new RuntimeException("Failed to connect to async server.", e);
        }
    }

    public void leave() {
        MembershipCounter membershipCounter = new MembershipCounter(storage);
        if (membershipCounter.isLeaving()) {
            System.out.println("Node is not in the cluster.");
            return;
        }
        incrementCounter(membershipCounter);

        try (MulticastConnection clusterConnection = new MulticastConnection(mcastAddr, mcastPort)) {
            clusterConnection.send(MembershipMessageProtocol.leave(membershipCounter.get()));
            System.out.println("message sent");
        } catch (IOException e) {
            throw new RuntimeException("Failed to connect to multicast group.", e);
        }
    }

    public void receive(ThreadPoolExecutor executor) {
        MulticastConnection clusterConnection = null;
        try {
            clusterConnection = new MulticastConnection(mcastAddr, mcastPort);
        } catch (IOException e) {
            throw new RuntimeException("Failed to connect to multicast group.", e);
        }

        executor.submit(new MembershipReceiverHandler(clusterConnection));
    }
}
