import Connection.MulticastConnection;
import Membership.MembershipCounter;
import Membership.MembershipHandler;
import Membership.MembershipService;
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
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

public class Node implements MembershipService {
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
    public void join() throws RemoteException {
        System.out.println("Node joined");

        MembershipHandler membershipHandler = new MembershipHandler(storage, mcastAddr, mcastPort, storePort);
        membershipHandler.join();
        // TODO add running thread for tcp connections

    }

    @Override
    public void leave() {
        System.out.println("Node left");
    }

    public void bindRMI(String name) {
        MembershipService membershipService = null;
        try {
            membershipService = (MembershipService) UnicastRemoteObject.exportObject(this, 0);
            Registry registry = LocateRegistry.getRegistry();
            registry.rebind(name, membershipService);
        } catch (RemoteException e) {
            try {
                Registry registry = LocateRegistry.createRegistry(Registry.REGISTRY_PORT);
                registry.rebind(name, membershipService);
            } catch (RemoteException ex) {
                System.err.println("Failed to connect to registry.");
                return;
            }
        }
        System.out.println("Server ready");
    }
}
