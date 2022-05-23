package Membership;

import Connection.MulticastConnection;
import Message.MembershipMessageProtocol;
import Storage.PersistentStorage;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.rmi.RemoteException;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

public class MembershipHandler implements MembershipService{
    private static final int MAX_MEMBERSHIP_MESSAGES = 3;
    private static final int MEMBERSHIP_ACCEPT_TIMEOUT = 500;
    private static final int MAX_RETRANSMISSION_TIMES = 3;
    private PersistentStorage storage;
    private String mcastAddr;
    private int mcastPort;
    private int storePort;

    ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newCachedThreadPool();

    public MembershipHandler(PersistentStorage storage, String mcastAddr, int mcastPort, int storePort) {
        this.storage = storage;
        this.mcastAddr = mcastAddr;
        this.mcastPort = mcastPort;
        this.storePort = storePort;
    }

    private ServerSocket initializeServerSocket() {
        ServerSocket serverSocket = null;
        try {
            serverSocket = new ServerSocket(this.storePort);
            serverSocket.setSoTimeout(MEMBERSHIP_ACCEPT_TIMEOUT);
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

    private void sendMembershipMessageProtocolMulticast(MulticastConnection clusterConnection, int count) {
        try {
            clusterConnection.send(MembershipMessageProtocol.join(count, this.storePort));
        } catch (IOException e) {
            throw new RuntimeException("Failed to connect to multicast group.", e);
        }
    }

    @Override
    public void join() throws RemoteException {
        MembershipCounter membershipCounter = new MembershipCounter(storage);

        if (membershipCounter.isJoinCount()) {
            System.out.println("Node is already in the cluster.");
            return;
        }

        ServerSocket serverSocket = initializeServerSocket();

        System.out.println("Server is listening on port " + this.storePort);

        try {
            membershipCounter.increment();
        } catch (IOException e) {
            throw new RuntimeException("Failed to increment membership counter in non-volatile memory.", e);
        }

        MulticastConnection clusterConnection = initializeMulticastConnection();
        sendMembershipMessageProtocolMulticast(clusterConnection, membershipCounter.get());
        int transmissionCount = 1;
        int membershipMessagesCount = 0;

        while (membershipMessagesCount < MAX_MEMBERSHIP_MESSAGES
                && transmissionCount < MAX_RETRANSMISSION_TIMES) {
            try {
                Socket socket = serverSocket.accept();
                membershipMessagesCount += 1;
                executor.submit(new ParseMembershipTask(socket));
            } catch (SocketTimeoutException ex) {
                System.out.println("There was a timeout, trying again");
                sendMembershipMessageProtocolMulticast(clusterConnection, membershipCounter.get());
                transmissionCount += 1;

            } catch (IOException ex) {
                System.out.println("Server exception: " + ex.getMessage());
                ex.printStackTrace();
                transmissionCount = MAX_RETRANSMISSION_TIMES;
            } catch (NullPointerException ex) {
                System.out.println("Server was not initialized: " + ex.getMessage());
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

    }
}
