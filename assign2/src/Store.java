import java.rmi.AccessException;
import java.rmi.AlreadyBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

public class Store {
    private static void printUsage() {
        System.out.println("java Store <IP_mcast_addr> <IP_mcast_port> <node_id>  <Store_port>");
    }

    private static void startStore(String mcastAddr, int mcastPort, String nodeId, int storePort) {
        PersistentStorage storage = new PersistentStorage(nodeId);
        Node node = new Node(storage);
        try {
            MembershipService membershipService = (MembershipService) UnicastRemoteObject.exportObject(node, 0);
            Registry registry = LocateRegistry.getRegistry();
            registry.rebind("Membership", membershipService);

            System.err.println("Server ready");
        } catch (RemoteException e) {
            System.out.println("Failed to connect to registry.");
        }
    }

    public static void main(String[] args) {
        if (args.length != 4) {
            printUsage();
            return;
        }

        String mcast_addr, node_id;
        int mcast_port, store_port;

        mcast_addr = args[0];
        try {
            mcast_port = Integer.parseInt(args[1]);
        } catch (NumberFormatException e) {
            System.out.println("Invalid mcast port " + args[1] + ".");
            printUsage();
            return;
        }
        node_id = args[2];
        try {
            store_port = Integer.parseInt(args[3]);
        } catch (NumberFormatException e) {
            System.out.println("Invalid store port " + args[1] + ".");
            printUsage();
            return;
        }

        startStore(mcast_addr, mcast_port, node_id, store_port);
    }
}
