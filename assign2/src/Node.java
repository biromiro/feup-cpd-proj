import java.io.IOException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

public class Node implements MembershipService {
    private final PersistentStorage storage;
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

        // TODO start accepting connections

        int count;
        try {
            count = membershipCounter.increment();
        } catch (IOException e) {
            throw new RuntimeException("Failed to increment membership counter in non-volatile memory.", e);
        }

        try (MulticastConnection clusterConnection = new MulticastConnection(mcastAddr, mcastPort)) {
            clusterConnection.send(MembershipMessageProtocol.join(count));
        } catch (IOException e) {
            throw new RuntimeException("Failed to connect to multicast group.", e);
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
