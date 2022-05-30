import KVStore.Cluster;
import Membership.*;
import Storage.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

public class Node implements MembershipService {
    private final String mcastAddr;
    private final int mcastPort;
    private final String nodeId;
    private final int storePort;
    private int priority;
    private final MembershipCounter membershipCounter;
    private final MembershipView membershipView;

    private final MembershipHandler membershipHandler;
    private final ThreadPoolExecutor executor;

    public Node(PersistentStorage storage, String mcastAddr, int mcastPort,
                String nodeId, int storePort) {
        this.mcastAddr = mcastAddr;
        this.mcastPort = mcastPort;
        this.nodeId = nodeId;
        this.storePort = storePort;

        this.membershipCounter = new MembershipCounter(storage);
        MembershipLog membershipLog = new MembershipLog(storage);
        this.membershipView = new MembershipView(membershipLog);

        this.executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        this.membershipHandler = new MembershipHandler(mcastAddr, mcastPort, nodeId, storePort, executor);
    }

    private void incrementCounter() {
        try {
            membershipCounter.increment();
        } catch (IOException e) {
            throw new RuntimeException("Failed to increment membership counter in non-volatile memory.", e);
        }

        this.membershipView.updateMember(nodeId, membershipCounter.get());
    }

    @Override
    public void join() throws RemoteException {
        if (membershipCounter.isJoin()) {
            System.out.println("Node is already in the cluster.");
            return;
        }
        incrementCounter();

        membershipHandler.join(membershipCounter.get());
        membershipHandler.receive(executor, membershipView);
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

    /*
        testclient: pede get a access point (x)
        access point: calcula successor e pede-lhe keyvalue
        successor: responde com keyvalue

        sou sucessor? sim -> respondo
                      nao -> peço ao sucessor
     */

    public void get(String key) {
        Cluster cluster = membershipView.getMembers();
        String successor = cluster.successor(key);
        //KVStoreHandler getter = new KVStoreHandler();
        //TCP connection to get entry from there
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

    public void initializeTCPLoop() {
        try (AsynchronousServerSocketChannel listener =
                     AsynchronousServerSocketChannel.open().bind(new InetSocketAddress(this.storePort))){
            listener.accept(null, new CompletionHandler<AsynchronousSocketChannel,Void>() {
                public void completed(AsynchronousSocketChannel ch, Void att) {
                    // accept the next connection
                    listener.accept(null, this);

                    // handle this connection
                    executor.submit(new ClusterChangeMessageHandler(ch));
                }
                public void failed(Throwable exc, Void att) {

                }
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void start() {
        this.initializeTCPLoop();
        if (membershipCounter.isJoin()) {
            membershipHandler.receive(executor, membershipView);
        }
        /* TODO else reinitialize, send multicast saying a crash occurred, asking for 3 membership logs, start
            sending multicast as cluster leader, receive in tcp loop the message from cluster leader becoming its predecessor
            */
    }
}
