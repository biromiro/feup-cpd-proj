import java.rmi.AccessException;
import java.rmi.AlreadyBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

public class Store {
    public static void main(String[] args) {
        Node node = new Node();

        try {
            MembershipService membershipService = (MembershipService) UnicastRemoteObject.exportObject(node, 0);
            Registry registry = LocateRegistry.getRegistry();
            registry.rebind("Membership", membershipService);

            System.err.println("Server ready");
        } catch (RemoteException e) {
            System.out.println("Failed to connect to registry.");
        }
    }
}
