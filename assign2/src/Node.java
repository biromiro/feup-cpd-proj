import Connection.MulticastConnection;
import Membership.*;
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
        membershipHandler.join();
        // TODO get information from predecessor
    }

    @Override
    public void leave() throws RemoteException {
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

    public void initializeMulticastLoop() {
        membershipHandler.receive(executor);
    }
}
