import Connection.MulticastConnection;
import Membership.*;
import Message.MembershipLog;
import Message.MembershipMessageProtocol;
import Storage.PersistentStorage;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
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
    private static final int N_THREADS = 3;

    private final String mcastAddr;
    private final int mcastPort;
    private final String nodeId;
    private final int storePort;

    private final MembershipCounter membershipCounter;
    private final MembershipLog membershipLog;

    private final MembershipHandler membershipHandler;
    private final ThreadPoolExecutor executor;

    public Node(PersistentStorage storage, String mcastAddr, int mcastPort,
                String nodeId, int storePort) {
        this.mcastAddr = mcastAddr;
        this.mcastPort = mcastPort;
        this.nodeId = nodeId;
        this.storePort = storePort;

        this.membershipCounter = new MembershipCounter(storage);
        this.membershipLog = new MembershipLog(storage);

        this.membershipHandler = new MembershipHandler(mcastAddr, mcastPort, nodeId, storePort);
        this.executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(N_THREADS);
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
        membershipHandler.receive(executor, membershipLog);
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
            membershipHandler.receive(executor, membershipLog);
        }
    }
}
