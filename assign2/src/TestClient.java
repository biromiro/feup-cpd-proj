import Connection.TcpConnection;
import KVStore.KVEntry;
import Membership.MembershipService;
import Message.ClientServerMessageProtocol;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

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
        try (TcpConnection connection = new TcpConnection(ip, port)) {
            switch (operation) {
                case "put" -> put(connection, operand);
                case "get" -> get(connection, operand);
                case "delete" -> delete(connection, operand);
            }
        } catch (IOException e) {
            System.out.println("Faulty connection to node at ip address " + ip + ":" + port);
        }
    }

    private static void put(TcpConnection connection, String filepath) {
        File file = new File(filepath);
        StringBuilder data = new StringBuilder();
        try (Scanner scanner = new Scanner(file)){
            while (scanner.hasNextLine()) {
                data.append(scanner.nextLine());
            }
        } catch (FileNotFoundException e) {
            System.out.println("File " + filepath + "does not exist.");
            return;
        }
        String value = data.toString();
        String key = KVEntry.hash(value); // TODO "compute key from value" :D:D:D:D:D:D:D:D
        connection.send(ClientServerMessageProtocol.put(key, value));
        System.out.println("Put value with key " + key);
    }

    private static void get(TcpConnection connection, String key) throws IOException {
        connection.send(ClientServerMessageProtocol.get(key));
        String value = connection.read();
        System.out.println(value);
    }

    private static void delete(TcpConnection connection, String key) {
        connection.send(ClientServerMessageProtocol.delete(key));
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
