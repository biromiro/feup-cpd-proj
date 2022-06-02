package Membership;

import Connection.AsyncServer;
import Connection.AsyncTcpConnection;
import Connection.MulticastConnection;
import Message.MembershipMessageProtocol;
import Message.MessageProtocolException;

import java.io.IOException;
import java.nio.channels.AsynchronousSocketChannel;
import java.util.Map;
import java.util.concurrent.*;

public class MembershipHandler {
    private static final int MAX_MEMBERSHIP_MESSAGES = 3;
    private static final int MEMBERSHIP_ACCEPT_TIMEOUT = 1000;
    private static final int MAX_RETRANSMISSION_TIMES = 3;
    private static final long WAIT_OTHERS_DELAY_MILLISECONDS = 200;
    private static final long TIME_BETWEEN_MEMBERSHIP_MULTICASTS = 1000;
    private final String mcastAddr;
    private final int mcastPort;
    private final String nodeId;
    private final MembershipView membershipView;
    private MulticastConnection clusterConnection;
    private final ScheduledThreadPoolExecutor executor;

    public MembershipHandler(String mcastAddr, int mcastPort, String nodeId,
                             MembershipView membershipView, ScheduledThreadPoolExecutor executor) {
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

        Map<String, Integer> blacklist = new ConcurrentHashMap<>();
        String message;
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
                membershipMessagesCount += 1;
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
                this.sendMulticastMembership(0);
            }

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
        try (AsyncServer serverSocket = new AsyncServer(executor)) {
            if (clusterConnection.isClosed()) clusterConnection = new MulticastConnection(mcastAddr, mcastPort);
            if (!connectToCluster(serverSocket, clusterConnection, MembershipMessageType.REINITIALIZE, 0)) {
                this.sendMulticastMembership(0);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to connect to async server upon reinitialize message.", e);
        }
    }

    public void sendMulticastMembership(long delay) {
        membershipView.becomeMulticasterCandidate(
                executor.scheduleWithFixedDelay(
                        new MembershipEchoMessageSender(clusterConnection, membershipView),
                        delay, TIME_BETWEEN_MEMBERSHIP_MULTICASTS, TimeUnit.MILLISECONDS
                )
        );
    }

    public void tryToAssumeMulticasterRole() {
        long delay = membershipView.getIndexInCluster() * WAIT_OTHERS_DELAY_MILLISECONDS;
        this.sendMulticastMembership(delay);
    }
}
