import Connection.AsyncServer;
import Connection.AsyncTcpConnection;
import KVStore.KVStoreMessageHandler;
import Membership.*;
import Storage.*;

import java.io.IOException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

public class Node implements MembershipService {
    private final String nodeId;
    private final int storePort;
    private final MembershipCounter membershipCounter;
    private final MembershipView membershipView;
    private final MembershipHandler membershipHandler;
    private final ThreadPoolExecutor executor;
    private static final int NUM_THREADS_PER_CORE = 4;

    public Node(PersistentStorage storage, String mcastAddr, int mcastPort,
                String nodeId, int storePort) {
        this.nodeId = nodeId;
        this.storePort = storePort;

        this.membershipCounter = new MembershipCounter(storage);
        MembershipLog membershipLog = new MembershipLog(storage);
        this.membershipView = new MembershipView(membershipLog, this.nodeId);

        // TODO how to choose the number of threads? max(processors, 32) or processors*4 or something else?
        int numberThreads = Runtime.getRuntime().availableProcessors() * NUM_THREADS_PER_CORE;
        System.out.println("There are " + numberThreads + " threads in the pool.");
        this.executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(numberThreads);
        this.membershipHandler = new MembershipHandler(mcastAddr, mcastPort, nodeId, membershipView, executor);
    }

    private void incrementCounter() {
        try {
            membershipCounter.increment();
        } catch (IOException e) {
            throw new RuntimeException("Failed to increment membership counter in non-volatile memory.", e);
        }
    }

    @Override
    public void join() throws RemoteException {
        if (membershipCounter.isJoin()) {
            // TODO faz sentido isto ser dito no cliente ou no servidor?
            System.out.println("Node is already in the cluster.");
            return;
        }
        incrementCounter();

        membershipHandler.join(membershipCounter.get());
        membershipHandler.receive();
        // TODO get information from predecessor
    }

    @Override
    public void leave() throws RemoteException {
        if (membershipCounter.isLeave()) {
            System.out.println("Node is not in the cluster.");
            return;
        }
        incrementCounter();

        // TODO transfer information to successor
        membershipHandler.leave(membershipCounter.get());
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
            }
        }
    }

    public void initializeTCPLoop() {
        AsyncServer listener;
        try {
            listener = new AsyncServer(this.storePort, executor);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        listener.loop(new AsyncServer.ConnectionHandler() {
            @Override
            public void completed(AsyncTcpConnection channel) {
                // TODO  receive in tcp loop the message from cluster leader becoming its predecessor
                // TODO does it make sense to submit a thread here? Async IO takes care of it, right?
                executor.submit(new KVStoreMessageHandler(nodeId, channel, membershipView));
            }

            @Override
            public void failed(Throwable exc) { exc.printStackTrace(); }
        });
    }

    public void start() {
        this.initializeTCPLoop();
        if (membershipCounter.isJoin()) {
            membershipHandler.reinitialize();
            membershipHandler.receive();
        }
    }
}
