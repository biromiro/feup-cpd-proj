package Membership;

import Message.MembershipMessageProtocol;
import Message.MessageProtocolException;

import java.util.Random;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class MembershipProtocolHandler implements Runnable {
    private static final int MEMBERSHIP_THRESHOLD_SEND = 10;
    private final String receivedMessage;
    private final MembershipView membershipView;
    private final ThreadPoolExecutor executor;
    private final MembershipHandler membershipHandler;

    public MembershipProtocolHandler(String receivedMessage, MembershipView membershipView, ThreadPoolExecutor executor,
                                     MembershipHandler membershipHandler) {
        this.receivedMessage = receivedMessage;
        this.membershipView = membershipView;
        this.executor = executor;
        this.membershipHandler = membershipHandler;
    }

    @Override
    public void run() {
        MembershipMessageProtocol parsedMessage;
        try {
            parsedMessage = MembershipMessageProtocol.parse(receivedMessage);
        } catch (MessageProtocolException e) {
            e.printStackTrace();
            return;
        }

        if (parsedMessage instanceof MembershipMessageProtocol.Join joinMessage) {
            if (!joinMessage.getId().equals(membershipView.getNodeId())) {
                handleJoin(joinMessage);
            }
        } else if (parsedMessage instanceof MembershipMessageProtocol.Leave leaveMessage) {
            if (!leaveMessage.getId().equals(membershipView.getNodeId())) {
                handleLeave(leaveMessage);
            }
        } else if (parsedMessage instanceof MembershipMessageProtocol.Membership membershipMessage) {
            if (!membershipMessage.getId().equals(membershipView.getNodeId())) {
                handleMembership(membershipMessage);
            }

        } else if (parsedMessage instanceof MembershipMessageProtocol.Reinitialize reinitializeMessage) {
            if (!reinitializeMessage.getId().equals(membershipView.getNodeId())) {
                handleReinitialize(reinitializeMessage);
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
        // TODO send membership
        //int indexDiff = getMemberIndex(joinMessage.getId()) - membershipView.getBroadcasterIndex();
        //if (indexDiff < MEMBERSHIP_THRESHOLD_SEND) {
        //    sendMembershipRandWait(indexDiff, joinMessage.getPort());
        //}
    }

    private void handleLeave(MembershipMessageProtocol.Leave leaveMessage) {
        membershipView.updateMember(leaveMessage.getId(), leaveMessage.getMembershipCounter());
        // TODO
    }

    private void handleMembership(MembershipMessageProtocol.Membership membershipMessage) {
        membershipView.merge(membershipMessage.getLog());

        int idCompare = membershipMessage.getId().compareTo(membershipView.getNodeId());
        if (idCompare < 0) {
            //membershipView.setBroadcasterIndex(getMemberIndex(membershipMessage.getId()));
            membershipHandler.sendBroadcastMembership();
        } else if (idCompare > 0) {
            membershipView.stopBroadcasting();
        }
        // TODO
    }

    private void handleReinitialize(MembershipMessageProtocol.Reinitialize reinitializeMessage) {
        // TODO maybe should send the membership counter as well on reinitialize message?
        // TODO send membership
        //int indexDiff = getMemberIndex(reinitializeMessage.getId()) - membershipView.getBroadcasterIndex();
        //if (indexDiff < MEMBERSHIP_THRESHOLD_SEND) {
        //    sendMembershipRandWait(indexDiff, reinitializeMessage.getPort());
        //}
    }
}
