package Membership;

import Message.MembershipMessageProtocol;
import Message.MessageProtocolException;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class MembershipProtocolHandler implements Runnable {
    private static final int MEMBERSHIP_THRESHOLD_SEND = 5;
    private static final double MAX_SEND_MEMBERSHIP_VIEW_DELAY_MILLISECONDS = 1000;
    private final String receivedMessage;
    private final MembershipView membershipView;
    private final ThreadPoolExecutor executor;
    private final MembershipHandler membershipHandler;

    public MembershipProtocolHandler(String receivedMessage, MembershipView membershipView,
                                     ThreadPoolExecutor executor, MembershipHandler handler) {
        this.receivedMessage = receivedMessage;
        this.membershipView = membershipView;
        this.executor = executor;
        this.membershipHandler = handler;
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

    private void sendMembershipView(String host, int port) {
        int delay = (int) (Math.random() * MAX_SEND_MEMBERSHIP_VIEW_DELAY_MILLISECONDS);
        CompletableFuture.delayedExecutor(delay, TimeUnit.MILLISECONDS).execute(() -> executor.submit(new MembershipMessageDispatcher(executor, membershipView, host, port)));
    }

    private void determineIfShouldSendMembershipView(String host, int port, Map<String, Integer> blacklist) {
        if (Objects.equals(blacklist.get(membershipView.getNodeId()), membershipView.getCount())) return;
        if (membershipView.isMulticasting()) {
            sendMembershipView(host, port);
        } else {
            double probability = Math.min(1.0,
                    (double) MEMBERSHIP_THRESHOLD_SEND / (membershipView.getCluster().size() - 1));
            if (Math.random() < probability) {
                sendMembershipView(host, port);
            }
        }
    }

    private void handleJoin(MembershipMessageProtocol.Join joinMessage) {
        membershipView.updateMember(joinMessage.getId(), joinMessage.getMembershipCounter());

        this.determineIfShouldSendMembershipView(joinMessage.getId(), joinMessage.getPort(), joinMessage.getBlacklist());
    }

    private void handleLeave(MembershipMessageProtocol.Leave leaveMessage) {
        membershipView.updateMember(leaveMessage.getId(), leaveMessage.getMembershipCounter());
    }

    private void handleMembership(MembershipMessageProtocol.Membership membershipMessage) {
        boolean outdatedMessage = membershipView.merge(membershipMessage.getLog());

        if (outdatedMessage) {
            membershipHandler.tryToAssumeMulticasterRole();
        } else {
            int idCompare = membershipMessage.getId().compareTo(membershipView.getNodeId());
            if (idCompare < 0) {
                membershipView.stopMulticasting();
            }
        }
    }

    private void handleReinitialize(MembershipMessageProtocol.Reinitialize reinitializeMessage) {
        // TODO maybe should send the membership counter as well on reinitialize message?
        this.determineIfShouldSendMembershipView(
                reinitializeMessage.getId(),
                reinitializeMessage.getPort(),
                reinitializeMessage.getBlacklist());
    }
}
