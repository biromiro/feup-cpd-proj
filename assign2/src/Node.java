import Connection.AsyncServer;
import Connection.AsyncTcpConnection;
import KVStore.BucketTransferrer;
import KVStore.Cluster;
import KVStore.KVStoreMessageHandler;
import Membership.*;
import Storage.*;

import java.io.IOException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;

public class Node implements MembershipService {
    private final String nodeId;
    private final int storePort;
    private final MembershipCounter membershipCounter;
    private final MembershipView membershipView;
    private final MembershipHandler membershipHandler;
    private final Bucket bucket;
    private final BucketTransferrer transferrer;
    private final ScheduledThreadPoolExecutor executor;
    private static final int NUM_THREADS_PER_CORE = 4;

    public Node(String mcastAddr, int mcastPort, String nodeId, int storePort) {
        this.nodeId = nodeId;
        this.storePort = storePort;

        int numberThreads = Runtime.getRuntime().availableProcessors() * NUM_THREADS_PER_CORE;
        System.out.println("There are " + numberThreads + " threads in the pool.");
        this.executor = (ScheduledThreadPoolExecutor) Executors.newScheduledThreadPool(numberThreads);

        PersistentStorage storage = new PersistentStorage(nodeId, "storage", this.executor);
        this.bucket = new Bucket(storage);
        this.membershipCounter = new MembershipCounter(storage);
        MembershipLog membershipLog = new MembershipLog(storage);
        Cluster cluster = new Cluster();
        this.transferrer = new BucketTransferrer(this.nodeId, bucket, cluster, executor, storePort);
        this.membershipView = new MembershipView(membershipLog, this.nodeId, cluster, transferrer);

        this.membershipHandler = new MembershipHandler(mcastAddr, mcastPort, this.nodeId, membershipView, executor);
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
            System.out.println("Node is already in the cluster.");
            return;
        }
        incrementCounter();

        membershipHandler.join(membershipCounter.get());
        membershipHandler.receive();
    }

    @Override
    public void leave() throws RemoteException {
        if (membershipCounter.isLeave()) {
            System.out.println("Node is not in the cluster.");
            return;
        }
        incrementCounter();

        transferrer.transfer((_null) -> {
            membershipHandler.leave(membershipCounter.get());
            return null;
        });
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
            listener = new AsyncServer(this.nodeId, this.storePort, executor);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        listener.loop(new AsyncServer.ConnectionHandler() {
            @Override
            public void completed(AsyncTcpConnection channel) {
                // TODO  receive in tcp loop the message from cluster leader becoming its predecessor
                new KVStoreMessageHandler(membershipCounter.isJoin(), nodeId, channel, bucket, membershipView)
                        .run();
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
