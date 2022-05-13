import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Arrays;
import java.util.List;

public class TestClient {
    private static final List<String> MEMBERSHIP_OPERATIONS = Arrays.asList("join", "leave");
    private static final List<String> KEY_VALUE_OPERATIONS = Arrays.asList("put", "get", "delete");

    private static void printUsage() {
        System.out.println("java TestClient <node_ap_ip>:<remote_object> <operation> [<operand>]");
    }

    private static void membershipOperation(String ip, String remoteObject, String operation) {
        try {
            Registry registry = LocateRegistry.getRegistry(ip);
            MembershipService membershipService = (MembershipService) registry.lookup(remoteObject);
            switch (operation) {
                case "join" -> membershipService.join();
                case "leave" -> membershipService.leave();
            }
        } catch (RemoteException e) {
            System.out.println("Failed to connect to registry with ip " + ip + ".");
        } catch (NotBoundException e) {
            System.out.println("'" + remoteObject + "' is not bound in registry with ip " + ip + ".");
        }
    }

    private static void keyValueOperation(String ip, int port, String operation, String operand) {
        switch (operation) {
            case "put" -> put(ip, port, operand);
            case "get" -> get(ip, port, operand);
            case "delete" -> delete(ip, port, operand);
        }
    }

    private static void put(String ip, int port, String filepath) {
        // TODO
    }

    private static void get(String ip, int port, String key) {
        // TODO
    }

    private static void delete(String ip, int port, String key) {
        // TODO
    }

    public static void main(String[] args) {
        if (args.length < 2) {
            printUsage();
            return;
        }

        String[] accessPoint = args[0].split(":", 2);
        if (accessPoint.length != 2) {
            printUsage();
            return;
        }

        String ip = accessPoint[0];
        String operation = args[1];

        if(MEMBERSHIP_OPERATIONS.contains(operation) && args.length == 2) {
            String remoteObject = accessPoint[1];
            membershipOperation(ip, remoteObject, operation);
        } else if(KEY_VALUE_OPERATIONS.contains(operation) && args.length == 3) {
            int port = Integer.parseInt(accessPoint[1]);
            String operand = args[2];
            keyValueOperation(ip, port, operation, operand);
        } else {
            printUsage();
        }
    }
}
