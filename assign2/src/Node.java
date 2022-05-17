import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

public class Node implements MembershipService {
    private static final int MAX_MEMBERSHIP_MESSAGES = 3;
    private static final int MEMBERSHIP_ACCEPT_TIMEOUT = 500;
    private static final int MAX_RETRANSMISSION_TIMES = 3;
    private PersistentStorage storage;
    private String mcastAddr;
    private int mcastPort;
    private String nodeId;
    private int storePort;

    public Node(PersistentStorage storage, String mcastAddr, int mcastPort, String nodeId, int storePort) {
        this.storage = storage;
        this.mcastAddr = mcastAddr;
        this.mcastPort = mcastPort;
        this.nodeId = nodeId;
        this.storePort = storePort;
    }

    @Override
    public void join() {
        System.out.println("Node joined");
        MembershipCounter membershipCounter = new MembershipCounter(storage);

        if (membershipCounter.get() % 2 == 0) {
            System.out.println("Node is already in the cluster.");
            return;
        }

        ServerSocket serverSocket = null;
        try {
            serverSocket = new ServerSocket(this.storePort);
            serverSocket.setSoTimeout(MEMBERSHIP_ACCEPT_TIMEOUT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("Server is listening on port " + this.storePort);

        int count;
        try {
            count = membershipCounter.increment();
        } catch (IOException e) {
            throw new RuntimeException("Failed to increment membership counter in non-volatile memory.", e);
        }

        try (MulticastConnection clusterConnection = new MulticastConnection(mcastAddr, mcastPort)) {
            clusterConnection.send(MembershipMessageProtocol.join(count, this.storePort));
        } catch (IOException e) {
            throw new RuntimeException("Failed to connect to multicast group.", e);
        }

        int membershipMessagesCount = 0;
        int transmissionCount = 0;
        while (membershipMessagesCount < MAX_MEMBERSHIP_MESSAGES) {
            try {
                transmissionCount += 1;
                Socket socket = serverSocket.accept();
                membershipMessagesCount += 1;

                InputStream input = socket.getInputStream();
                BufferedReader reader = new BufferedReader(new InputStreamReader(input));

                String time = reader.readLine();
                System.out.println("New MEMBERSHIP message: " + time);
                // TODO parse membership message
            } catch (SocketTimeoutException ex) {
                if (transmissionCount >= MAX_RETRANSMISSION_TIMES) {
                    System.out.println("Max retrasmissions, giving up");
                } else {
                    System.out.println("There was a timeout, trying again");
                }
            } catch (IOException ex) {
                System.out.println("Server exception: " + ex.getMessage());
                ex.printStackTrace();
                transmissionCount = 3;
                try {
                    serverSocket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
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
    public void leave() {
        System.out.println("Node left");
    }

    public void bindRMI(String name) {
        try {
            MembershipService membershipService = (MembershipService) UnicastRemoteObject.exportObject(this, 0);
            Registry registry = LocateRegistry.getRegistry();
            registry.rebind(name, membershipService);

            System.err.println("Server ready");
        } catch (RemoteException e) {
            System.out.println("Failed to connect to registry.");
        }
    }
}
