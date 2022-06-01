package Membership;

import Connection.AsyncServer;
import Connection.AsyncTcpConnection;
import Connection.MulticastConnection;
import Message.MembershipMessageProtocol;
import Message.MessageProtocolException;

import java.io.IOException;
import java.nio.channels.AsynchronousSocketChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;

public class MembershipHandler {
    private static final int MAX_MEMBERSHIP_MESSAGES = 3;
    private static final int MEMBERSHIP_ACCEPT_TIMEOUT = 1000;
    private static final int MAX_RETRANSMISSION_TIMES = 3;
    private static final int WAIT_OTHERS_DELAY_MILLISECONDS = 200;
    private final String mcastAddr;
    private final int mcastPort;
    private final String nodeId;
    private final MembershipView membershipView;
    private MulticastConnection clusterConnection;
    private final ThreadPoolExecutor executor;

    public MembershipHandler(String mcastAddr, int mcastPort, String nodeId,
                             MembershipView membershipView, ThreadPoolExecutor executor) {
        this.mcastAddr = mcastAddr;
        this.mcastPort = mcastPort;
        this.nodeId = nodeId;
        this.membershipView = membershipView;
        try {
            this.clusterConnection = new MulticastConnection(mcastAddr, mcastPort);
        } catch (IOException e) {
            throw new RuntimeException("Failed to connect to multicast group.", e);
        }
        this.executor = executor;
    }

    private String getMessage(MembershipMessageType type, int port, int count, Map<String, Integer> blacklist)
            throws MessageProtocolException {
        if (type == MembershipMessageType.JOIN) {
            return MembershipMessageProtocol.join(this.nodeId, port, count, blacklist);
        } else if (type == MembershipMessageType.REINITIALIZE) {
            return MembershipMessageProtocol.reinitialize(this.nodeId, port, blacklist);
        }

        throw new MessageProtocolException("Unexpected message type '" + type + '\'');

    }

    private boolean connectToCluster(
            AsyncServer serverSocket,
            MulticastConnection clusterConnection,
            MembershipMessageType type, int count) throws IOException {
        String message = null;
        Map<String, Integer> blacklist = new ConcurrentHashMap<>();

        try {
            message = this.getMessage(type, serverSocket.getPort(), count, blacklist);
        } catch (MessageProtocolException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
        clusterConnection.send(message);

        int transmissionCount = 1;
        int membershipMessagesCount = 0;

        Future<AsynchronousSocketChannel> future = serverSocket.accept();
        while (membershipMessagesCount < MAX_MEMBERSHIP_MESSAGES) {
            try {
                AsynchronousSocketChannel worker = future.get(MEMBERSHIP_ACCEPT_TIMEOUT, TimeUnit.MILLISECONDS);
                membershipMessagesCount += 1; // TODO only increment this if message received was actually a MEMBERSHIP
                if (membershipMessagesCount < MAX_MEMBERSHIP_MESSAGES) {
                    future = serverSocket.accept();
                }

                System.out.println("Received membership message from " + worker.getRemoteAddress());

                executor.submit(new MembershipMessageHandler(new AsyncTcpConnection(worker), membershipView, blacklist));
            } catch (TimeoutException e) {
                transmissionCount += 1;
                if (transmissionCount > MAX_RETRANSMISSION_TIMES) {
                    System.out.println("Limit of retransmissions reached.");
                    break;
                } else {
                    System.out.println("There was a timeout, trying again");
                    try {
                        message = this.getMessage(type, serverSocket.getPort(), count, blacklist);
                    } catch (MessageProtocolException ex) {
                        throw new RuntimeException(ex.getMessage(), ex);
                    }
                    // TODO nodes that have already replied shouldn't reply again, UNLESS their membership view changed
                    clusterConnection.send(message);
                }
            } catch (InterruptedException ex) {
                System.out.println("Server was not initialized: " + ex.getMessage());
            } catch (ExecutionException ex) {
                System.out.println("Server exception: " + ex.getMessage());
                break;
            }
        }
        serverSocket.close();

        return membershipMessagesCount != 0;
    }

    public void join(int count) {
        try (AsyncServer serverSocket = new AsyncServer(executor)) {
            if (clusterConnection.isClosed()) clusterConnection = new MulticastConnection(mcastAddr, mcastPort);
            if (!connectToCluster(serverSocket, clusterConnection, MembershipMessageType.JOIN, count)) {
                membershipView.updateMember(nodeId, count);
                this.sendBroadcastMembership(0);
            }
            //if (membershipView.isBroadcaster() && !membershipView.isBroadcasting()) {
            //    this.sendBroadcastMembership();
            //}

        } catch (IOException e) {
            throw new RuntimeException("Failed to connect to async server.", e);
        }
    }

    public void leave(int count) {
        try  {
            membershipView.stopMulticasting();
            clusterConnection.leave();
            clusterConnection.send(MembershipMessageProtocol.leave(this.nodeId, count));
            clusterConnection.close();
            membershipView.updateMember(nodeId, count);
        } catch (IOException e) {
            throw new RuntimeException("Failed to connect to multicast group.", e);
        }
    }

    public void receive() {
        executor.submit(new MembershipProtocolDispatcher(this, clusterConnection, executor, membershipView));
    }

    public void reinitialize() {
        // TODO send multicast saying a crash occurred, asking for 3 membership logs
        try (AsyncServer serverSocket = new AsyncServer(executor)) {
            if (clusterConnection.isClosed()) clusterConnection = new MulticastConnection(mcastAddr, mcastPort);
            if (!connectToCluster(serverSocket, clusterConnection, MembershipMessageType.REINITIALIZE, 0)) {
                this.sendBroadcastMembership(0);
            }
            //if (membershipView.isBroadcaster() && !membershipView.isBroadcasting()) this.sendBroadcastMembership();
        } catch (IOException e) {
            throw new RuntimeException("Failed to connect to async server upon reinitialize message.", e);
        }
    }

    public void sendBroadcastMembership(int delay) {
        if (!membershipView.mayMulticast()) {
            membershipView.becomeMulticasterCandidate();
            if (delay == 0) {
                membershipView.startMulticasting();
                executor.submit(new MembershipEchoMessageSender(executor, clusterConnection, membershipView));
            } else {
                // TODO isto cria um novo executor. Nos so deviamos usar o executor original
                CompletableFuture.delayedExecutor(delay, TimeUnit.MILLISECONDS).execute(() -> {
                    membershipView.startMulticasting();
                    executor.submit(new MembershipEchoMessageSender(executor, clusterConnection, membershipView));
                });
            }
        }
    }

    public void tryToAssumeMulticasterRole() {
        int delay = membershipView.getIndexInCluster() * WAIT_OTHERS_DELAY_MILLISECONDS;
        this.sendBroadcastMembership(delay);
    }
}
