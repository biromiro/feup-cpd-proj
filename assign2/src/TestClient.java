import Connection.TcpConnection;
import KVStore.KVEntry;
import Membership.MembershipService;
import Message.ClientServerMessageProtocol;
import Message.MessageProtocolException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.*;
import java.util.function.Function;

public class TestClient {
    private static final List<String> MEMBERSHIP_OPERATIONS = Arrays.asList("join", "leave");
    private static final List<String> KEY_VALUE_OPERATIONS = Arrays.asList("put", "get", "delete");

    private static void printUsage() {
        System.out.println("java TestClient <node_ap_ip>:<store_port> <operation> [<operand>]");
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
            case "delete" -> delete(ip, port, operand);
            case "get" -> get(ip, port, operand);
        }
    }

    private static void put(String ip, int port, String filepath) {
        String content;
        try {
            content = Files.readString(Paths.get(filepath));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        KVEntry entry = new KVEntry(content);
        actOnEndpoint(ip, port, ClientServerMessageProtocol.put(entry));
    }

    private static void delete(String ip, int port, String key) {
        actOnEndpoint(ip, port, ClientServerMessageProtocol.delete(key));
    }

    private static void get(String ip, int port, String key) {
        actOnEndpoint(ip, port, ClientServerMessageProtocol.get(key), (message) -> {
            System.out.println(message.getBody());
            return null;
        });
    }

    private static void actOnEndpoint(String ip, int port, String request) {
        actOnEndpoint(ip, port, request, (message) -> null);
    }
    private static void actOnEndpoint(String ip, int port, String request, Function<ClientServerMessageProtocol.Done, Void> onDone) {
        List<String> visited = new ArrayList<>();
        Queue<String> toVisit = new ArrayDeque<>();
        toVisit.add(ip + ":" + port);
        while (!toVisit.isEmpty()) {
            String visiting = toVisit.remove();
            System.out.println("start " + visiting);
            visited.add(visiting);
            String[] params = visiting.split(":");
            ip = params[0];
            port = Integer.parseInt(params[1]);

            try (TcpConnection connection = new TcpConnection(ip, port)) {
                //Ask for entry
                connection.send(request);

                // Hear response
                String value;
                value = connection.read();

                // Process it
                try {
                    ClientServerMessageProtocol answer = ClientServerMessageProtocol.parse(value);

                    if (answer instanceof ClientServerMessageProtocol.Done doneMessage) {
                        //No redirection. At the endpoint node.
                        onDone.apply(doneMessage);
                        break;
                    }

                    else if (answer instanceof ClientServerMessageProtocol.Error error) {
                        System.out.println("Error: " + error.getErrorMessage());
                    }

                    else if (answer instanceof ClientServerMessageProtocol.Redirect) {
                        //Not at endpoint node. Reach them instead.
                        List<String> hosts = ((ClientServerMessageProtocol.Redirect) answer).getHosts();
                        List<Integer> ports = ((ClientServerMessageProtocol.Redirect) answer).getPorts();
                        for (int i = 0; i < hosts.size(); i++) {
                            String potentialNode = hosts.get(i) + ":" + ports.get(i);
                            if (!visited.contains(potentialNode))
                                toVisit.add(potentialNode);
                        }
                    }

                    else {
                        System.out.println("Unexpected answer to request");
                    }
                } catch (MessageProtocolException e) {
                    throw new RuntimeException(e);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
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
