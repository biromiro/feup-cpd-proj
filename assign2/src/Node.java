import Connection.MulticastConnection;
import Membership.MembershipCounter;
import Membership.MembershipDispatcher;
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
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

public class Node implements MembershipService {
    private static final int N_THREADS = 3;
    private final MembershipHandler membershipHandler;
    private PersistentStorage storage;
    private String mcastAddr;
    private int mcastPort;
    private String nodeId;
    private int storePort;

    private ThreadPoolExecutor executor;

    public Node(PersistentStorage storage, String mcastAddr, int mcastPort, String nodeId, int storePort) {
        this.storage = storage;
        this.mcastAddr = mcastAddr;
        this.mcastPort = mcastPort;
        this.nodeId = nodeId;
        this.storePort = storePort;
        this.executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(N_THREADS);
        this.membershipHandler = new MembershipHandler(storage, mcastAddr, mcastPort, storePort, executor);

    }

    @Override
    public void join() throws RemoteException {
        System.out.println("Node joined");

        membershipHandler.join();
        // TODO get information from predecessor
        // TODO add running thread for tcp connections
        executor.submit(new MembershipDispatcher(storage, storePort, executor));

    }

    @Override
    public void leave() throws RemoteException {
        System.out.println("Node left");

        // TODO transfer information to successor

        membershipHandler.leave();

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
