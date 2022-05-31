package Membership;

import Connection.AsyncTcpConnection;
import Message.MembershipMessageProtocol;
import Message.MessageProtocolException;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class MembershipProtocolHandler implements Runnable {
    private static final int MEMBERSHIP_THRESHOLD_SEND = 10;
    private final String receivedMessage;
    private final MembershipView membershipView;
    private final ThreadPoolExecutor executor;

    public MembershipProtocolHandler(String receivedMessage, MembershipView membershipView, ThreadPoolExecutor executor) {
        this.receivedMessage = receivedMessage;
        this.membershipView = membershipView;
        this.executor = executor;
    }

    @Override
    public void run() {
        System.out.println("Gonna parse it");
        MembershipMessageProtocol parsedMessage;
        try {
            parsedMessage = MembershipMessageProtocol.parse(receivedMessage);
        } catch (MessageProtocolException e) {
            throw new RuntimeException("Invalid message", e);
        }

        // System.out.println("Parsed it");
        if (parsedMessage instanceof MembershipMessageProtocol.Join joinMessage) {
            if (!joinMessage.getId().equals(membershipView.getNodeId())) {
                System.out.println("received join message on port "
                        + joinMessage.getPort() + " with counter " + joinMessage.getMembershipCounter());
                handleJoin(joinMessage);
            }
        } else if (parsedMessage instanceof MembershipMessageProtocol.Leave leaveMessage) {
            if (!leaveMessage.getId().equals(membershipView.getNodeId())) {
                System.out.println("received leave message with counter " + leaveMessage.getMembershipCounter());
                handleLeave(leaveMessage);
            }
        } else if (parsedMessage instanceof MembershipMessageProtocol.Membership membershipMessage) {
            if (!membershipMessage.getId().equals(membershipView.getNodeId())) {
                System.out.println("received membership message");
                handleMembership(membershipMessage);
            }

        } else if (parsedMessage instanceof MembershipMessageProtocol.Reinitialize reinitializeMessage) {
            if (!reinitializeMessage.getId().equals(membershipView.getNodeId())) {
                System.out.println("received reinitialize message on port"
                        + reinitializeMessage.getPort());
                handleReinitialize(reinitializeMessage);
            }
        } else if (parsedMessage instanceof MembershipMessageProtocol.MembershipLog membershipLogMessage) {
            if (!membershipLogMessage.getId().equals(membershipView.getNodeId())) {
                System.out.println("received membership log message");
                handleMembershipLog(membershipLogMessage);
            }
        }
    }

    private int getMemberIndex(String nodeId) {
        return membershipView.getMembers().stream().sorted().toList().indexOf(nodeId);
    }



    private void sendMembershipRandWait(int indexDiff, int port) {
        ScheduledThreadPoolExecutor scheduledExecutor = (ScheduledThreadPoolExecutor) executor;
        scheduledExecutor.schedule(
                new MembershipMessageDispatcher(executor, membershipView, port),
                (long) indexDiff *  (new Random()).nextInt(50), TimeUnit.MILLISECONDS);
    }

    private void handleJoin(MembershipMessageProtocol.Join joinMessage) {
        membershipView.updateMember(joinMessage.getId(), joinMessage.getMembershipCounter());
        int indexDiff = getMemberIndex(joinMessage.getId()) - membershipView.getBroadcasterIndex();
        if ( indexDiff < MEMBERSHIP_THRESHOLD_SEND) {
            sendMembershipRandWait(indexDiff, joinMessage.getPort());
        }
    }

    private void handleLeave(MembershipMessageProtocol.Leave leaveMessage) {
        membershipView.updateMember(leaveMessage.getId(), leaveMessage.getMembershipCounter());
        // TODO
    }

    private void handleMembership(MembershipMessageProtocol.Membership membershipMessage) {
        membershipView.merge(membershipMessage.getMembers(), membershipMessage.getLog());
        // TODO
    }

    private void handleReinitialize(MembershipMessageProtocol.Reinitialize reinitializeMessage) {
        // TODO maybe should send the membership counter as well on reinitialize message?
        int indexDiff = getMemberIndex(reinitializeMessage.getId()) - membershipView.getBroadcasterIndex();
        if ( indexDiff < MEMBERSHIP_THRESHOLD_SEND) {
            sendMembershipRandWait(indexDiff, reinitializeMessage.getPort());
        }
    }

    private void handleMembershipLog(MembershipMessageProtocol.MembershipLog membershipLogMessage) {
        membershipView.merge(null, membershipLogMessage.getLog());
        if (membershipLogMessage.getId().compareTo(membershipView.getNodeId()) < 0 )
            membershipView.setBroadcasterIndex(getMemberIndex(membershipLogMessage.getId()));

    }
}
