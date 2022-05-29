package Membership;

import Connection.MulticastConnection;
import Message.MembershipMessageProtocol;
import Storage.PersistentStorage;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.rmi.RemoteException;
import java.util.concurrent.*;

public class MembershipHandler implements MembershipService{
    private static final int MAX_MEMBERSHIP_MESSAGES = 3;
    private static final int MEMBERSHIP_ACCEPT_TIMEOUT = 500;
    private static final int MAX_RETRANSMISSION_TIMES = 3;
    private PersistentStorage storage;
    private String mcastAddr;
    private int mcastPort;
    private int storePort;

    private ThreadPoolExecutor executor;

    public MembershipHandler(PersistentStorage storage, String mcastAddr, int mcastPort, int storePort, ThreadPoolExecutor executor) {
        this.storage = storage;
        this.mcastAddr = mcastAddr;
        this.mcastPort = mcastPort;
        this.storePort = storePort;
    }

    private AsynchronousServerSocketChannel initializeServerSocket() {
        AsynchronousServerSocketChannel serverSocket = null;
        try {
            serverSocket = AsynchronousServerSocketChannel.open().bind(new InetSocketAddress(this.storePort));
            // serverSocket.setSoTimeout();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return serverSocket;
    }

    private MulticastConnection initializeMulticastConnection() {
        MulticastConnection clusterConnection = null;
        try {
            clusterConnection = new MulticastConnection(mcastAddr, mcastPort);
        } catch (IOException e) {
            throw new RuntimeException("Failed to connect to multicast group.", e);
        }

        return clusterConnection;
    }

    private void sendJoinMulticast(MulticastConnection clusterConnection, int count) {
        try {
            clusterConnection.send(MembershipMessageProtocol.join(count, this.storePort));
            System.out.println("message sent");
        } catch (IOException e) {
            throw new RuntimeException("Failed to connect to multicast group.", e);
        }
    }

    private void sendLeaveMulticast(MulticastConnection clusterConnection, int count) {
        try {
            clusterConnection.send(MembershipMessageProtocol.leave(count));
            System.out.println("message sent");
        } catch (IOException e) {
            throw new RuntimeException("Failed to connect to multicast group.", e);
        }
    }

    @Override
    public void  join() throws RemoteException {
        MembershipCounter membershipCounter = new MembershipCounter(storage);

        if (membershipCounter.isJoinCount()) {
            System.out.println("Node is already in the cluster.");
            return;
        }

        AsynchronousServerSocketChannel serverSocket = initializeServerSocket();

        System.out.println("Server is listening on port " + this.storePort);

        try {
            membershipCounter.increment();
        } catch (IOException e) {
            throw new RuntimeException("Failed to increment membership counter in non-volatile memory.", e);
        }

        MulticastConnection clusterConnection = initializeMulticastConnection();
        sendJoinMulticast(clusterConnection, membershipCounter.get());
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
                sendJoinMulticast(clusterConnection, membershipCounter.get());
                transmissionCount += 1;
            } catch (InterruptedException ex) {
                System.out.println("Server was not initialized: " + ex.getMessage());
            } catch (ExecutionException ex) {
                System.out.println("Server exception: " + ex.getMessage());
                ex.printStackTrace();
                transmissionCount = MAX_RETRANSMISSION_TIMES;
            }
        }

        System.out.println("Goodbye socket for membership");
        try {
            serverSocket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void leave() throws RemoteException {
        MembershipCounter membershipCounter = new MembershipCounter(storage);

        if (membershipCounter.isLeaveCount()) {
            System.out.println("Node is not in the cluster.");
            return;
        }

        try {
            membershipCounter.increment();
        } catch (IOException e) {
            throw new RuntimeException("Failed to increment membership counter in non-volatile memory.", e);
        }

        MulticastConnection clusterConnection = initializeMulticastConnection();
        sendLeaveMulticast(clusterConnection, membershipCounter.get());

        System.out.println("Goodbye socket for membership");
    }

    public void receive(ThreadPoolExecutor executor) {
        MulticastConnection clusterConnection = initializeMulticastConnection();
        executor.submit(new MembershipReceiverHandler(clusterConnection));
    }
}
