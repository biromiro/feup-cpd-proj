package Membership;

import Storage.PersistentStorage;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ThreadPoolExecutor;

public class MembershipDispatcher implements Runnable{

    ThreadPoolExecutor executor;
    PersistentStorage storage;
    int storePort;
    public MembershipDispatcher(PersistentStorage storage, int storePort, ThreadPoolExecutor executor) {
        this.storage = storage;
        this.storePort = storePort;
        this.executor = executor;
    }

    @Override
    public void run() {
        ServerSocket serverSocket = null;
        try {
            serverSocket = new ServerSocket(this.storePort);
        } catch (IOException e) {
            e.printStackTrace();
        }

        // while not leave
        while (true) {
            Socket socket = null;
            try {
                socket = serverSocket.accept();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            executor.submit(new ClusterChangeMessageHandler(socket));
        }


    }
}
